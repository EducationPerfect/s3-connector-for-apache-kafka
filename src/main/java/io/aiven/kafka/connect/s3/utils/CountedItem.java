package io.aiven.kafka.connect.s3.utils;

/**
 * An item that is associated with a count and indicates whether it is the last item in a sequence.
 */
public final class CountedItem<T> {
    public final int itemNumber;
    public final Boolean isLast;
    public final T item;

    public CountedItem(int itemNumber, Boolean isLast, T item) {
        this.itemNumber = itemNumber;
        this.isLast = isLast;
        this.item = item;
    }
}
