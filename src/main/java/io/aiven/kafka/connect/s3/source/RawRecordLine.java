package io.aiven.kafka.connect.s3.source;

import java.util.Objects;

public final class RawRecordLine {
    private final SourceFileInfo source;
    private final S3Offset offset;
    private final String line;

    public RawRecordLine(SourceFileInfo source, S3Offset offset, String line) {
        this.source = source;
        this.offset = offset;
        this.line = line;
    }

    public SourceFileInfo source() {
        return source;
    }

    public S3Offset offset() {
        return offset;
    }

    public String line() {
        return line;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RawRecordLine) obj;
        return Objects.equals(this.source, that.source) &&
                Objects.equals(this.offset, that.offset) &&
                Objects.equals(this.line, that.line);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, offset, line);
    }

    @Override
    public String toString() {
        return "RawRecordLine[" +
                "source=" + source + ", " +
                "offset=" + offset + ", " +
                "line=" + line + ']';
    }

}
