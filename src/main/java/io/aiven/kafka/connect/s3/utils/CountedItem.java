package io.aiven.kafka.connect.s3.utils;

import java.util.Objects;

/**
 * An item that is associated with a count and indicates whether it is the last item in a sequence.
 */
public final class CountedItem<T> {
    public final long itemNumber;
    public final Boolean isLast;
    public final T item;

    public CountedItem(long itemNumber, Boolean isLast, T item) {
        this.itemNumber = itemNumber;
        this.isLast = isLast;
        this.item = item;
    }
}
