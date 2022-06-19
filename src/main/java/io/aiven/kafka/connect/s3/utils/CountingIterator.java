package io.aiven.kafka.connect.s3.utils;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CountingIterator is a wrapper around an iterator that counts items.
 * It also returns a flag whether an item is the last one.
 */
public final class CountingIterator<T> implements Iterator<CountedItem<T>> {
    private final Iterator<T> iterator;
    private final AtomicLong startCount;

    public static <T> Iterator<CountedItem<T>> of(Iterator<T> iterator, long startCount) {
        return new CountingIterator<>(startCount, iterator);
    }

    public CountingIterator(long startCount, Iterator<T> iterator) {
        this.iterator = iterator;
        this.startCount = new AtomicLong(startCount);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public CountedItem<T> next() {
        var item = iterator.next();
        return new CountedItem<>(this.startCount.getAndIncrement(), this.hasNext(), item);
    }
}
