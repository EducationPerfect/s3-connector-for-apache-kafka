package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterators;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.*;
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
    private static final String CURRENT_OBJECT_KEY = "current_file_name";
    private static final String LAST_PROCESSED_OBJECT_KEY = "last_processed_file_name";
    private static final String LINE_NUMBER = "line_number";
    private static final String PARTITION_KEY = "partition_key";

    private FilenameParser filenameParser;

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
        filenameParser = new FilenameParser(config.getFilenameTemplate().toString());

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

        final var name = mOffset.get(CURRENT_OBJECT_KEY);
        final var last = mOffset.get(LAST_PROCESSED_OBJECT_KEY);
        final var lineNumber = mOffset.get(LINE_NUMBER);

        final var offset = (name != null && lineNumber != null) ? new S3Offset((String)last, (String)name, (Long)lineNumber) : null;

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

        for (var partition : partitionsOffsets) {
            var stream = S3PartitionLines.readLines(s3Client, filenameParser, partition.partition, partition.offset);

            var its = Iterators.partition(stream.iterator(), 100);

//                    .forEachRemaining(lines -> {
//                        lines.stream().map(x -> {
//                            try {
//                                return buildSourceRecord(partition.partition, x);
//                            } catch (JsonProcessingException ex) {
//                                throw new RuntimeException(ex.getMessage());
//                            }
//                        });
//                    });
        }

//        var fileLines = "{\"test\": \"asd\"}\n{\"test\": \"qwe\"}\n".split("\\R");
//
//        var result = new ArrayList<SourceRecord>();
//        try {
//            for (long lineNumber = 0; lineNumber < fileLines.length; lineNumber++) {
//                var sourceRecord = buildSourceRecord(JsonRecordParser.parse(fileLines[(int) lineNumber]), "testFile.jsonl", lineNumber);
//                result.add(sourceRecord);
//            }
//        }
//        catch (JsonProcessingException ex)
//        {
//            LOGGER.error("An error occurred while processing file");
//        }

        return null;
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Source Task");
    }

    private SourceRecord buildSourceRecord(S3Partition partition, S3PartitionLines.Line line) throws JsonProcessingException {
        String topic = config.getTopic();

        Map<String, Object> sourceOffset = buildSourceOffset(line.offset());
        Map<String, Object> sourcePartition = buildSourcePartition(partition);

        var record = JsonRecordParser.parse(line.line());

        ConnectHeaders headers = new ConnectHeaders();
        for (var headerIndex = 0; headerIndex < record.headers().length; headerIndex++) {
            var header = record.headers()[headerIndex];
            headers.add(header.key(), new SchemaAndValue(null, header.value()));
        }

        return new SourceRecord(
            sourcePartition, sourceOffset,
            topic, line.source().partition(),
            null, record.key(),
            null, record.value(),
            record.timestamp().getMillis(), headers
        );
    }

    private Map<String, Object> buildSourcePartition(S3Partition partition) {
        return Collections.singletonMap(PARTITION_KEY, null);
    }

    private Map<String, Object> buildSourceOffset(S3Offset offset) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put(CURRENT_OBJECT_KEY, offset.currentKey());
        sourceOffset.put(LAST_PROCESSED_OBJECT_KEY, offset.lastFullyProcessedKey());
        sourceOffset.put(LINE_NUMBER, offset.offset());
        return sourceOffset;
    }
}
