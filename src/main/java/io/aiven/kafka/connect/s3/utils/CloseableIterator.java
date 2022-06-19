package io.aiven.kafka.connect.s3.utils;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

}
