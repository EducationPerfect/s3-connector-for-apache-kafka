package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.*;
import io.aiven.kafka.connect.s3.utils.CollectionUtils;
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
import java.util.stream.Collectors;

public class S3SourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);
    private static final String OFFSET_FILENAME = "offset.filename";
    private static final String OFFSET_LINE_NUMBER = "offset.line.number";
    private static final String PARTITION_BUCKET_NAME = "partition.bucket.name";
    private static final String PARTITION_PREFIX = "partition.prefix";

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
                .map(s -> {
                    var partition = new S3Partition(config.getAwsS3BucketName(), s);
                    var offset = readStoredOffset(context.offsetStorageReader(), partition);
                    return new PartitionStream(partition, offset, null);
                })
                .collect(Collectors.toList());

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
    private PartitionStream refreshBatches(PartitionStream partition) {
        if (partition.remainingBatches == null) {
            var linesStream = S3PartitionLines.readLines(s3Client, partition.partition, filenameParser, partition.lastKnownOffset, config.getFilesPageSize());
            var batches = StreamUtils.batching(config.getBatchSize(), linesStream);
            return partition.withBatches(StreamUtils.asClosableIterator(batches));
        } else {
            return partition;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Objects.requireNonNull(this.context, "context hasn't been set");
        Objects.requireNonNull(this.context.offsetStorageReader(), "offsetStorageReader hasn't been set");

        // take next partition from the top of the queue and put it to the back so that others will have turns
        var partitionToProcess = partitionsQueue.poll();
        Objects.requireNonNull(partitionToProcess, "Panic: partitionsQueue is empty");

        var current = refreshBatches(partitionToProcess);

        var nextBatch = IteratorUtils.getNext(current.remainingBatches, List.of());
        var lastOffset = CollectionUtils.last(nextBatch).map(RawRecordLine::offset).orElse(current.lastKnownOffset);

        if (!nextBatch.isEmpty()) {
            LOGGER.debug("Processing batch of {}, last offset: {}", nextBatch.size(), lastOffset);
        }

        var sourceRecords = nextBatch
                .stream()
                .map(r -> {
                    try {
                        return buildSourceRecord(current.partition, r);
                    } catch (JsonProcessingException ex) {
                        throw new RuntimeException(ex.getMessage());
                    }
                })
                .collect(Collectors.toList());

        // if there are no more batches, then we put the partition back to the FRONT of the queue
        // so that we keep iterating on it until it is finished.
        // Otherwise, we put it to the BACK of the queue, so that others will have turns.
        if (current.remainingBatches.hasNext()) {
            partitionsQueue.addFirst(current.withLastKnownOffset(lastOffset));
        } else {
            try {
                current.remainingBatches.close();
            } catch (Exception e) {
                throw new InterruptedException(e.getMessage());
            }
            partitionsQueue.addLast(current.withLastKnownOffset(lastOffset).withBatches(null));
        }

        // to avoid accessing the bucket at a crazy speed when there is no new data
        if (sourceRecords.isEmpty()) {
            Thread.sleep(config.getEmptyPollDelayMs());
        }

        return sourceRecords;
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Source Task");
    }

    private SourceRecord buildSourceRecord(S3Partition partition, RawRecordLine line) throws JsonProcessingException {
        String topic = config.getTopic();

        Map<String, Object> sourceOffset = toSourceRecordOffset(line.offset());
        Map<String, Object> sourcePartition = toSourceRecordPartition(partition);

        var record = RecordLine.parseJson(line.line());

        ConnectHeaders headers = new ConnectHeaders();

        if (record.headers() != null) {
            for (var header : record.headers()) {
                headers.add(header.key, new SchemaAndValue(null, header.value));
            }
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
                OFFSET_FILENAME, offset.filename(),
                OFFSET_LINE_NUMBER, offset.offset()
        );
    }

    private static Map<String, Object> toSourceRecordPartition(S3Partition partition) {
        return Map.of(
                PARTITION_BUCKET_NAME, partition.bucket(),
                PARTITION_PREFIX, partition.prefix()
        );
    }

    private static S3Offset readStoredOffset(OffsetStorageReader reader, S3Partition partition) {
        final var mPart = toSourceRecordPartition(partition);
        final var mOffset = reader.offset(mPart);

        if (mOffset == null) { return null; }
        else {
            final var lastProcessed = mOffset.get(OFFSET_FILENAME);
            final var lineNumber = mOffset.get(OFFSET_LINE_NUMBER);

            return (lastProcessed != null && lineNumber != null) ? new S3Offset((String) lastProcessed, (long)lineNumber) : null;
        }
    }
}
