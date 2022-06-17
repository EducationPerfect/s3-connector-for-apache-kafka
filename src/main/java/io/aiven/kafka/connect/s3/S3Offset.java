package io.aiven.kafka.connect.s3;

public record S3Offset(String lastFullyProcessedKey, long offset) { }
