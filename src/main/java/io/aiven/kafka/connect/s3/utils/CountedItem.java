package io.aiven.kafka.connect.s3.utils;

/**
 * An item that is associated with a count and indicates whether it is the last item in a sequence.
 * @param itemNumber the item number
 * @param isLast whether this is the last item in a sequence
 * @param item the item
 * @param <T> the type of the item
 */
public record CountedItem<T>(long itemNumber, Boolean isLast, T item) {
}
