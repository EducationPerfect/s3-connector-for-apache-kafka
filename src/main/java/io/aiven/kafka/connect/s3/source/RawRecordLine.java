package io.aiven.kafka.connect.s3.source;

public record RawRecordLine(SourceFileInfo source, S3Offset offset, String line) {
}
