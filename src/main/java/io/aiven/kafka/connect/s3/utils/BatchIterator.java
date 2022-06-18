package io.aiven.kafka.connect.s3.utils;

import com.google.common.collect.Iterators;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class BatchIterator<T> implements Iterator<List<T>> {
    private final int batchSize;
    private final Iterator<T> iterator;

    public static <T> Iterator<List<T>> of(int batchSize, Iterator<T> iterator) {
        return new BatchIterator<>(batchSize, iterator);
    }

    private BatchIterator(int batchSize, Iterator<T> iterator) {
        this.batchSize = batchSize;
        this.iterator = requireNonNull(iterator);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public List<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        List<T> batch = new ArrayList<>(batchSize);
        for (var i = 0; i < batchSize && iterator.hasNext(); i++) {
            batch.add(iterator.next());
        }

        return batch;
    }
}