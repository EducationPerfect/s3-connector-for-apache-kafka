package io.aiven.kafka.connect.s3;

public record S3Partition(String bucket, String prefix, int partition) { }
