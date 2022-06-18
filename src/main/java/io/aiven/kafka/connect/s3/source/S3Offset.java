package io.aiven.kafka.connect.s3.source;

public record S3Offset(String startAfterKey, long offset) { }
