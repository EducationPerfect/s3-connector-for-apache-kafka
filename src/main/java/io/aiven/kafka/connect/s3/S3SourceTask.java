package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.JsonRecordParser;
import io.aiven.kafka.connect.s3.source.S3Partition;
import io.aiven.kafka.connect.s3.source.S3PartitionLines;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class S3SourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);
    private static final String FILE_NAME = "file_name";
    private static final String LINE_NUMBER = "line_number";
    private static final String PARTITION_KEY = "partition_key";

    private S3SourceConfig config;
    private AmazonS3 s3Client;
    private String fileName;
    private long lineNumber;
    protected AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();
    private List<S3Partition> s3partitions;

    static boolean isNotNullOrBlank(String str) {
        return str != null && !str.trim().isEmpty();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SourceConfig(props);
        s3Client = AWS.createAmazonS3Client(config);
        String topic = config.getString(S3SourceConfig.TOPIC);


        s3partitions = Arrays.stream(config.getPartitionPrefixes()).map(s -> new S3Partition(config.getAwsS3BucketName(), s)).toList();

        Map<String, Object> persistedMap = null;
        if (context != null && context.offsetStorageReader() != null) {
            persistedMap = context.offsetStorageReader().offset(buildSourcePartition(s3partitions));
        }

        LOGGER.info("The persistedMap is {}", persistedMap);
        if (persistedMap != null) {
            String fileName = (String) persistedMap.get(FILE_NAME);
            if (isNotNullOrBlank(fileName)) {
                this.fileName = fileName;
            }

            Object lineNumber = persistedMap.get(LINE_NUMBER);
            if (lineNumber != null) {
                this.lineNumber = (long) lineNumber;
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        var configBucket = config.getAwsS3BucketName();
        var prefixes = config.getPartitionPrefixes();

        var fileLines = "{\"test\": \"asd\"}\n{\"test\": \"qwe\"}\n".split("\\R");
        var result = new ArrayList<SourceRecord>();
        try {
            for (long lineNumber = 0; lineNumber < fileLines.length; lineNumber++) {
                var sourceRecord = buildSourceRecord(JsonRecordParser.parse(fileLines[(int) lineNumber]), "testFile.jsonl", lineNumber);
                result.add(sourceRecord);
            }
        }
        catch (JsonProcessingException ex)
        {
            LOGGER.error("An error occurred while processing file");
        }

        return result;
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Source Task");
    }

    private SourceRecord buildSourceRecord(JsonRecordParser.RecordLine record, String fileName, Long lineNumber) {
        String topic = config.getTopic();

        Map<String, Object> sourceOffset = buildSourceOffset(fileName, lineNumber);
        Map<String, Object> sourcePartition = buildSourcePartition(topic);

        ConnectHeaders headers = new ConnectHeaders();
        for (var headerIndex = 0; headerIndex < record.headers().length; headerIndex++) {
            var header = record.headers()[headerIndex];
            headers.add(header.key(), new SchemaAndValue(null, header.value()));
        }

        return new SourceRecord(
            sourcePartition, sourceOffset,
            topic, null,
            null, record.key(),
            null, record.value(),
            record.timestamp().getMillis(), headers
        );
    }

    private Map<String, Object> buildSourcePartition(List<S3Partition> partitions) {
        return Collections.singletonMap(PARTITION_KEY, topicKey);
    }

    private Map<String, Object> buildSourceOffset(String fileName, Long lineNumber) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put(FILE_NAME, fileName);
        sourceOffset.put(LINE_NUMBER, lineNumber);
        return sourceOffset;
    }
}
