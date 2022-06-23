package io.aiven.kafka.connect.s3.source;

import java.util.Objects;

/**
 * S3Offset is a representation of a consumer position for a partition backup on S3
 */
public final class S3Offset {
    private final String filename;
    private final long offset;

    /**
     * @param filename the file name (object key) on S3 that is currently being consumed
     * @param offset the line number that is currently being consumed in a given file. Int.MAX_VALUE is used to represent the end of a file.
     */
    public S3Offset(String filename, long offset) {
        this.filename = filename;
        this.offset = offset;
    }

    public String filename() {
        return filename;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (S3Offset) obj;
        return Objects.equals(this.filename, that.filename) &&
                this.offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, offset);
    }

    @Override
    public String toString() {
        return "S3Offset[" +
                "filename=" + filename + ", " +
                "offset=" + offset + ']';
    }
}
