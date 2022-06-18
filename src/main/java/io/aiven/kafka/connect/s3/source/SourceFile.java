package io.aiven.kafka.connect.s3.source;

import java.util.Optional;

public record SourceFile(String filename, String topic, int partition, Optional<Long> offset) {
}
