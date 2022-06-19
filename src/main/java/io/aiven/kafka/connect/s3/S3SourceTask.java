package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.*;
import io.aiven.kafka.connect.s3.utils.CloseableIterator;
import io.aiven.kafka.connect.s3.utils.IteratorUtils;
import io.aiven.kafka.connect.s3.utils.StreamUtils;
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
    private static final String LAST_PROCESSED_OBJECT_KEY = "filename.last";
    private static final String LINE_NUMBER = "line.number";
    private static final String BUCKET_NAME = "bucket.name";
    private static final String PARTITION_KEY = "partition.key";

    private FilenameParser filenameParser;

    private S3SourceConfig config;
    private AmazonS3 s3Client;

    /**
     * We will be "rotating" partitions to ensure fair distribution.
     * The "top" partition is getting processed, and is returned to the bottom of the queue
     * if its current "stream" is fully processed, or to the top of the queue if there are
     * more items in its current "stream" to process.
     */
    private Deque<PartitionStream> partitionsQueue;


    @Override
    public String version() {
        return Version.VERSION;
    }

    /**
     * When handling a partition, we read data from its files and send it to the Kafka topic.
     * Connector's `poll()` method requires returning a batch for each `poll` invocation.
     * A batch per file would be a nice approximation, but it may not work or may not be
     * efficient in these conditions:
     *
     * 1. A file is too large to fit in a single batch.
     * 2. Files are small, and doing lots of "listObjects" is expensive and suboptimal.
     *
     * Because of that, for each partition we open a "page" of several files, which are then
     * transparently represented as a sequence of batches.
     *
     * The idea is that the partition remains "active" until it has "remaining batches" associated with it,
     * and when there are no more batches, the next is picked up for handling.
     *
     * The partitions then are circled in a queue: process a "page" for one partition, then for another one, etc.
     */
    private record PartitionStream(
            S3Partition partition,
            CloseableIterator<List<RawSourceRecord>> remainingBatches) {}

    @Override
    public void start(Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        Objects.requireNonNull(this.context, "context hasn't been set");
        Objects.requireNonNull(this.context.offsetStorageReader(), "offsetStorageReader hasn't been set");

        config = new S3SourceConfig(props);
        filenameParser = new FilenameParser(config.getFilenameTemplate().toString());

        s3Client = AWS.createAmazonS3Client(config);

        // Initialise all partition streams, each with no "remaining batches", of course
        var allPartitions = Arrays
                .stream(config.getPartitionPrefixes())
                .map(s -> new PartitionStream(new S3Partition(config.getAwsS3BucketName(), s), null))
                .toList();

        // Here is a queue in which partitions will be roted:
        // When we handle a batch and the partition has more batches to process,
        // then we put it back at the front of the queue,
        // so that it will be handled next time, until no more batches.
        // Otherwise, when there are no more batches, the partition is sent back to the end of the queue.
        partitionsQueue = new LinkedList<>(allPartitions);
    }

    /**
     *  Gets the batches iterator for a given partition.
     *  If there is no known batches iterator for the partition, a new one will be created.
     */
    private CloseableIterator<List<RawSourceRecord>> getBatches(PartitionStream partition) {
        if (partition.remainingBatches == null) {
            var offset = readStoredOffset(context.offsetStorageReader(), partition.partition());
            var linesStream = S3PartitionLines.readLines(s3Client, partition.partition(), filenameParser, offset, 10);
            var batches = StreamUtils.batching(100, linesStream);
            return StreamUtils.asClosableIterator(batches);
        } else {
            return partition.remainingBatches;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Objects.requireNonNull(this.context, "context hasn't been set");
        Objects.requireNonNull(this.context.offsetStorageReader(), "offsetStorageReader hasn't been set");

        // take next partition from the top of the queue and put it to the back so that others will have turns
        var partition = partitionsQueue.poll();
        Objects.requireNonNull(partition, "Panic: partitionsQueue is empty");

        var remainingBatches = getBatches(partition);

        var batch = IteratorUtils
                .getNext(remainingBatches, List.of())
                .stream()
                .map(r -> {
                    try {
                        return buildSourceRecord(partition.partition, r);
                    } catch (JsonProcessingException ex) {
                        throw new RuntimeException(ex.getMessage());
                    }
                })
                .toList();

        // if there are no more batches, then we put the partition back to the FRONT of the queue
        // so that we keep iterating on it until it is finished.
        // Otherwise, we put it to the BACK of the queue, so that others will have turns.
        if (remainingBatches.hasNext()) {
            partitionsQueue.addFirst(new PartitionStream(partition.partition(), remainingBatches));
        } else {
            try {
                remainingBatches.close();
            } catch (Exception e) {
                throw new InterruptedException(e.getMessage());
            }
            partitionsQueue.addLast(new PartitionStream(partition.partition(), null));
        }

        return batch;
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Source Task");
    }

    private SourceRecord buildSourceRecord(S3Partition partition, RawSourceRecord line) throws JsonProcessingException {
        String topic = config.getTopic();

        Map<String, Object> sourceOffset = toSourceRecordOffset(line.offset());
        Map<String, Object> sourcePartition = toSourceRecordPartition(partition);

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

    private Map<String, Object> toSourceRecordOffset(S3Offset offset) {
        return Map.of(
                LAST_PROCESSED_OBJECT_KEY, offset.startAfterKey(),
                LINE_NUMBER, offset.offset()
        );
    }

    private static Map<String, Object> toSourceRecordPartition(S3Partition partition) {
        return Map.of(
                BUCKET_NAME, partition.bucket(),
                PARTITION_KEY, partition.prefix()
        );
    }

    private static S3Offset readStoredOffset(OffsetStorageReader reader, S3Partition partition) {
        final var mPart = toSourceRecordPartition(partition);
        final var mOffset = reader.offset(mPart);

        final var lastProcessed = mOffset.get(LAST_PROCESSED_OBJECT_KEY);
        final var lineNumber = mOffset.get(LINE_NUMBER);

        return (lastProcessed != null && lineNumber != null) ? new S3Offset((String)lastProcessed, (Long)lineNumber) : null;
    }
}
