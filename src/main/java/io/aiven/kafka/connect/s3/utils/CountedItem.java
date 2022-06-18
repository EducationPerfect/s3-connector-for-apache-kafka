package io.aiven.kafka.connect.s3.utils;

public record CountedItem<T>(long itemNumber, Boolean isLast, T item) {
}
