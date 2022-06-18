package io.aiven.kafka.connect.s3.source;

public record RawSourceRecord(SourceFile source, S3Offset offset, String line) {
}
