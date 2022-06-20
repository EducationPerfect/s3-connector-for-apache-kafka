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
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.StringConverter;
import org.joda.time.DateTime;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.GZIPOutputStream;

import static io.aiven.kafka.connect.s3.config.AivenCommonS3Config.*;
import static io.aiven.kafka.connect.s3.config.S3SourceConfig.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public final class S3SourceTaskTest {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());
    private static String filenameTemplate = "aiven/{{topic}}/partition={{partition}}/{{topic}}+{{partition}}+{{start_offset:padding=true}}.json.gz";

    private static final String TEST_BUCKET = "test-bucket";

    private static final String TEST_TOPIC = "test-topic";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;
    private static Map<String, String> commonProperties;

    private SourceTaskContext mockedSourceTaskContext;
    private OffsetBackingStore offsetBackingStore;

    private static BucketAccessor testBucketAccessor;

    @BeforeAll
    public static void setUpClass() {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;
        System.out.println("S3 port: " + s3Port);

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(
                TOPIC, TEST_TOPIC,
                FILE_NAME_TEMPLATE_CONFIG, filenameTemplate,
                AWS_ACCESS_KEY_ID, "test_key_id",
                AWS_SECRET_ACCESS_KEY, "test_secret_key",
                AWS_S3_BUCKET, TEST_BUCKET,
                AWS_S3_ENDPOINT, "http://localhost:" + s3Port,
                AWS_S3_REGION, "ap-southeast-2"
        );

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(AWS_ACCESS_KEY_ID),
                commonProperties.get(AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                commonProperties.get(AWS_S3_ENDPOINT),
                commonProperties.get(AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();

        System.out.println("Buckets: " + s3Client.listBuckets());
    }

    @AfterAll
    public static void tearDownClass() {

        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        offsetBackingStore = new MemoryOffsetBackingStore();
        var offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, "test", new StringConverter(), new StringConverter());
        mockedSourceTaskContext = new WorkerSourceTaskContext(offsetReader);
        offsetBackingStore.start();
    }

    @AfterEach
    public void tearDown() {
        offsetBackingStore.stop();
//        s3Client.deleteBucket(TEST_BUCKET);
    }

    @Test
    public void should_poll_multiple_partitions() throws Exception {
        uploadFile(TEST_TOPIC, 0, 0L, 10);
        uploadFile(TEST_TOPIC, 0, 10L, 10);
        uploadFile(TEST_TOPIC, 1, 0L, 13);

        System.out.println("Files: ");
        s3Client.listObjects(TEST_BUCKET).getObjectSummaries().forEach(System.out::println);

        var sourceTask = new S3SourceTask();
        sourceTask.initialize(mockedSourceTaskContext);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIX, partitionPrefixes);

        sourceTask.start(taskProperties);

        var firstPartition = sourceTask.poll();
        assertEquals(20, firstPartition.size());

        var secondPartition = sourceTask.poll();
        assertEquals(13, secondPartition.size());
    }

    private void uploadFile(String topic, int partition, Long startOffset, int maxItems) {
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


    private static String generateFileContent(Long startOffset, int maxItems) {
        return LongStream
                .range(startOffset, startOffset + maxItems)
                .mapToObj(S3SourceTaskTest::createLine)
                .map(x -> {
                    try {
                        return jsonMapper.writeValueAsString(x);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.joining("\n"));
    }

    private static JsonRecordParser.RecordLine createLine(Long offset) {
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
