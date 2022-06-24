package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.s3.utils.CloseableIterator;

import java.util.List;

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
public final class PartitionStream {
    public final S3Partition partition;
    public final S3Offset lastKnownOffset;
    public final CloseableIterator<List<RawRecordLine>> remainingBatches;

    public PartitionStream(
            S3Partition partition,
            S3Offset lastKnownOffset,
            CloseableIterator<List<RawRecordLine>> remainingBatches) {
        this.partition = partition;
        this.lastKnownOffset = lastKnownOffset;
        this.remainingBatches = remainingBatches;
    }

    public PartitionStream withBatches(CloseableIterator<List<RawRecordLine>> batches) {
        return new PartitionStream(partition, lastKnownOffset, batches);
    }

    public PartitionStream withLastKnownOffset(S3Offset offset) {
        return new PartitionStream(partition, offset, remainingBatches);
    }
}