package io.aiven.kafka.connect.s3.source;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.S3SourceTask;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.GZIPOutputStream;

import static io.aiven.kafka.connect.common.config.AivenCommonConfig.FILE_NAME_TEMPLATE_CONFIG;
import static io.aiven.kafka.connect.s3.config.AivenCommonS3Config.*;
import static io.aiven.kafka.connect.s3.config.S3SourceConfig.TOPIC_TARGET_CONFIG;

public abstract class SourceTaskTestBase {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());
    protected static String filenameTemplate = "aiven/{{topic}}/partition={{partition}}/{{topic}}+{{partition}}+{{start_offset:padding=true}}.json.gz";

    protected static final String TEST_BUCKET = "test-bucket";

    protected static final String TEST_TOPIC = "test-topic";

    private static S3Mock s3Api;
    protected static AmazonS3 s3Client;
    protected static Map<String, String> commonProperties;

    protected SourceTaskContext sourceTaskContext;
    protected OffsetBackingStore offsetBackingStore;

    protected OffsetStorageWriter offsetWriter;

    protected static BucketAccessor testBucketAccessor;

    protected S3SourceTask sourceTask;

    @BeforeAll
    public static void setUpClass() {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;
        System.out.println("S3 port: " + s3Port);

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(
                TOPIC_TARGET_CONFIG, TEST_TOPIC,
                FILE_NAME_TEMPLATE_CONFIG, filenameTemplate,
                AWS_ACCESS_KEY_ID_CONFIG, "test_key_id",
                AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key",
                AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET,
                AWS_S3_ENDPOINT_CONFIG, "http://localhost:" + s3Port,
                AWS_S3_REGION_CONFIG, "ap-southeast-2",
                "schemas.enable", "false"
        );

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(AWS_ACCESS_KEY_ID_CONFIG),
                commonProperties.get(AWS_SECRET_ACCESS_KEY_CONFIG)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                commonProperties.get(AWS_S3_ENDPOINT_CONFIG),
                commonProperties.get(AWS_S3_REGION_CONFIG)
        ));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        System.out.println("Buckets: " + s3Client.listBuckets());
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    protected final List<SourceRecord> poll() throws Exception {
        var results = sourceTask.poll();
        if (!results.isEmpty()) {
            results.forEach(r -> offsetWriter.offset(r.sourcePartition(), r.sourceOffset()));
            offsetWriter.beginFlush();
            offsetWriter.doFlush((err, res) -> {
                if (err != null) System.out.println("ERR: " + err);
            }).get();
        }
        return results;
    }

    protected final List<SourceRecord> pollNoCommitOffsets() throws Exception {
        return sourceTask.poll();
    }

    @BeforeEach
    public void setUp() {
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();
        var jsonv = new JsonConverter();
        jsonv.configure(commonProperties, false);

        offsetBackingStore = new MemoryOffsetBackingStore();

        offsetWriter = new OffsetStorageWriter(offsetBackingStore, "test", jsonv, jsonv);
        var offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, "test", jsonv, jsonv);

        sourceTaskContext = new WorkerSourceTaskContext(offsetReader);
        offsetBackingStore.start();

        sourceTask = new S3SourceTask();
        sourceTask.initialize(sourceTaskContext);
    }

    @AfterEach
    public void tearDown() {
        sourceTask.stop();
        offsetBackingStore.stop();
        s3Client.deleteBucket(TEST_BUCKET);
    }

    protected final void createBackupFile(String topic, int partition, Long startOffset, int maxItems) {
        var fileName = Template.of(filenameTemplate)
                .instance()
                .bindVariable(FilenameTemplateVariable.TOPIC.name, x -> topic)
                .bindVariable(FilenameTemplateVariable.PARTITION.name, x -> Integer.toString(partition))
                .bindVariable(FilenameTemplateVariable.START_OFFSET.name, x -> Long.toString(startOffset))
                .render();
        var fileContent = generateFileContent(startOffset, maxItems);

        try {
            try (var bos = new ByteArrayOutputStream(fileContent.length())) {
                try (var gzip = new GZIPOutputStream(bos)) {
                    gzip.write(fileContent.getBytes());
                }
                try (var is = new ByteArrayInputStream(bos.toByteArray())) {
                    s3Client.putObject(TEST_BUCKET, fileName, is, new ObjectMetadata());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String generateFileContent(Long startOffset, int maxItems) {
        return LongStream
                .range(startOffset, startOffset + maxItems)
                .mapToObj(this::createLine)
                .map(x -> {
                    try {
                        return jsonMapper.writeValueAsString(x);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.joining("\n"));
    }

    protected JsonRecordParser.RecordLine createLine(Long offset) {
        var header = new JsonRecordParser.RecordHeader<>("source-offset", offset);
        var headers = new JsonRecordParser.RecordHeader[]{header};

        return new JsonRecordParser.RecordLine(
                offset,
                String.format("test-key-%d", offset),
                String.format("test-value-%d", offset).getBytes(),
                new DateTime(),
                headers);
    }
}


