package io.aiven.kafka.connect.s3.utils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class StreamUtils {
    /**
     * Returns a {@link Stream} that counts lines in a given source {@link Stream}.
     * @param startCount the starting line number
     * @param stream the source stream
     * @return a {@link Stream} of {@link CountedItem} elements for the source stream
     * @param <T> the type of the elements in the source stream
     */
    public static <T> Stream<CountedItem<T>> counting(long startCount, Stream<T> stream) {
        return counting(startCount, stream.iterator()).onClose(stream::close);
    }

    /**
     * Returns a {@link Stream} that counts lines in a given source {@link Iterator}.
     * @param startCount the starting line number
     * @param iterator the source iterator
     * @return a {@link Stream} of {@link CountedItem} elements for the source iterator
     * @param <T> the type of the elements in the source iterator
     */
    public static <T> Stream<CountedItem<T>> counting(long startCount, Iterator<T> iterator) {
        var innerIterator = CountingIterator.of(iterator, startCount);
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(innerIterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /**
     * Returns a {@link Stream} of batches of a given size from a given source {@link Stream}.
     * @param batchSize the size of the batches
     * @param stream the source stream
     * @return a {@link Stream} of batches of the given size from the source stream
     * @param <T> the type of the elements in the source stream
     */
    public static <T> Stream<List<T>> batching(int batchSize, Stream<T> stream) {
        return batching(batchSize, stream.iterator()).onClose(stream::close);
    }

    /**
     * Returns a {@link Stream} of batches of a given size from a given source {@link Iterator}.
     * @param batchSize the size of the batches
     * @param iterator the source iterator
     * @return a {@link Stream} of batches of the given size from the source iterator
     * @param <T> the type of the elements in the source iterator
     */
    public static <T> Stream<List<T>> batching(int batchSize, Iterator<T> iterator) {
        var innerIterator =  BatchIterator.of(batchSize, iterator);
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(innerIterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /**
     * Returns a {@link CloseableIterator} for a given {@link Stream}.
     * The source stream is closed when the iterator is closed.
     * @param stream the source stream
     * @return a {@link CloseableIterator} for the source stream
     * @param <T> the type of the elements in the source stream
     */
    public static <T> CloseableIterator<T> asClosableIterator(Stream<T> stream) {
        return new CloseableIterator<T>() {
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
            public void close() {
                stream.close();
            }
        };
    }
}
