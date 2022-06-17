package io.aiven.kafka.connect.s3.source;

import java.util.Optional;

public record SourceDataInfo(String topic, int partition, Optional<Long> offset) {
}
