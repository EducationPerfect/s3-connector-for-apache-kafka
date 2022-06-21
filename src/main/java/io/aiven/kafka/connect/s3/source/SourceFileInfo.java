package io.aiven.kafka.connect.s3.source;

import java.util.Optional;

/**
 * SourceFileInfo is a representation of a file (object) on S3
 * @param filename the file name (object key) on S3
 * @param topic the name of the Kafka topic this file is a backup for
 * @param partition the partition this file is a backup for
 * @param offset starting offset for the records in this backup file
 */
public record SourceFileInfo(String filename, String topic, int partition, Optional<Long> offset) {
}
