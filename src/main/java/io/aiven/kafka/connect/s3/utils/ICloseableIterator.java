package io.aiven.kafka.connect.s3.utils;

import java.io.Closeable;
import java.util.Iterator;

public interface ICloseableIterator<T> extends Iterator<T>, AutoCloseable {

}
