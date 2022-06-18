package io.aiven.kafka.connect.s3.utils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class StreamUtils {
    public static <T> Stream<CountedItem<T>> counting(long startCount, Stream<T> stream) {
        return counting(startCount, stream.iterator()).onClose(stream::close);
    }

    public static <T> Stream<CountedItem<T>> counting(long startCount, Iterator<T> iterator) {
        var innerIterator = CountingIterator.of(iterator, startCount);
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(innerIterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    public static <T> Stream<List<T>> batching(int batchSize, Stream<T> stream) {
        return batching(batchSize, stream.iterator()).onClose(stream::close);
    }

    public static <T> Stream<List<T>> batching(int batchSize, Iterator<T> iterator) {
        var innerIterator =  BatchIterator.of(batchSize, iterator);
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(innerIterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    public static <T> ICloseableIterator<T> asClosableIterator(Stream<T> stream) {
        return new ICloseableIterator<T>() {
            private final Iterator<T> iterator = stream.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void close() throws IOException {
                stream.close();
            }
        };
    }
}
