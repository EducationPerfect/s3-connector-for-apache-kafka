package io.aiven.kafka.connect.s3.utils;

import java.util.Iterator;

public final class IteratorUtils {

    /**
     * A safe function to get the next element from an iterator.
     * @param iterator the iterator to get the next element from
     * @param defaultValue the default value to return if the iterator is empty
     * @return the next element from the iterator or the default value if the iterator is empty
     * @param <T> the type of the element
     */
    public static <T> T getNext(Iterator<T> iterator, T defaultValue) {
        return iterator.hasNext() ? iterator.next() : defaultValue;
    }
}
