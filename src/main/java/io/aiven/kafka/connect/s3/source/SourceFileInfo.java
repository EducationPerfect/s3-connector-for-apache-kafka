package io.aiven.kafka.connect.s3.source;

import java.util.Objects;
import java.util.Optional;

/**
 * SourceFileInfo is a representation of a file (object) on S3
 */
public final class SourceFileInfo {
    private final String filename;
    private final String topic;
    private final int partition;
    private final Optional<Long> offset;

    /**
     * @param filename the file name (object key) on S3
     * @param topic the name of the Kafka topic this file is a backup for
     * @param partition the partition this file is a backup for
     * @param offset starting offset for the records in this backup file
     */
    public SourceFileInfo(String filename, String topic, int partition, Optional<Long> offset) {
        this.filename = filename;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String filename() {
        return filename;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public Optional<Long> offset() {
        return offset;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SourceFileInfo) obj;
        return Objects.equals(this.filename, that.filename) &&
                Objects.equals(this.topic, that.topic) &&
                this.partition == that.partition &&
                Objects.equals(this.offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, topic, partition, offset);
    }

    @Override
    public String toString() {
        return "SourceFileInfo[" +
                "filename=" + filename + ", " +
                "topic=" + topic + ", " +
                "partition=" + partition + ", " +
                "offset=" + offset + ']';
    }

}
