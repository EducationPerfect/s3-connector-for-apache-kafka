package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.JsonRecordParser;
import io.aiven.kafka.connect.s3.source.S3Offset;
import io.aiven.kafka.connect.s3.source.S3Partition;
import io.aiven.kafka.connect.s3.source.S3PartitionLines;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
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

    protected AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();
    private List<PartitionOffset> partitionsOffsets;

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
        Objects.requireNonNull(this.context, "context hasn't been set");
        Objects.requireNonNull(this.context.offsetStorageReader(), "offsetStorageReader hasn't been set");

        config = new S3SourceConfig(props);
        s3Client = AWS.createAmazonS3Client(config);

        partitionsOffsets = Arrays
                .stream(config.getPartitionPrefixes())
                .map(s -> new S3Partition(config.getAwsS3BucketName(), s))
                .map(p -> getOffsetFor(context.offsetStorageReader(), p))
                .filter(Objects::nonNull)
                .toList();
    }

    private record PartitionOffset(S3Partition partition, S3Offset offset) { }
    private PartitionOffset getOffsetFor(OffsetStorageReader reader, S3Partition partition) {
        final var mPart = fromS3Partition(partition);
        final var mOffset = reader.offset(mPart);

        final var name = mOffset.get(FILE_NAME);
        final var lineNumber = mOffset.get(LINE_NUMBER);

        final var offset = (name != null && lineNumber != null) ? new S3Offset((String)name, (Long)lineNumber) : null;

        return new PartitionOffset(partition, offset);
    }

    private Map<String, String> fromS3Partition(S3Partition partition) {
        var result = new HashMap<String, String>();
        result.put("bucket", partition.bucket());
        result.put("prefix", partition.prefix());
        return result;
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
