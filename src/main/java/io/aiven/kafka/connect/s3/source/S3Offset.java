package io.aiven.kafka.connect.s3.source;

public record S3Offset(String lastFullyProcessedKey, String currentKey, long offset) { }
