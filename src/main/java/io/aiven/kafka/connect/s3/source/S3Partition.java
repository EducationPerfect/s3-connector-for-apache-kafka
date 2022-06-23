package io.aiven.kafka.connect.s3.source;

import java.util.Objects;

public final class S3Partition {
    private final String bucket;
    private final String prefix;

    public S3Partition(String bucket, String prefix) {
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public String bucket() {
        return bucket;
    }

    public String prefix() {
        return prefix;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (S3Partition) obj;
        return Objects.equals(this.bucket, that.bucket) &&
                Objects.equals(this.prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, prefix);
    }

    @Override
    public String toString() {
        return "S3Partition[" +
                "bucket=" + bucket + ", " +
                "prefix=" + prefix + ']';
    }
}
