package io.aiven.kafka.connect.s3.utils;

import java.util.Iterator;

public final class IteratorUtils {
    public static <T> T getNext(Iterator<T> iterator, T defaultValue) {
        return iterator.hasNext() ? iterator.next() : defaultValue;
    }
}
