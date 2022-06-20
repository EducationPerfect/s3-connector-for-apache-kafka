package io.aiven.kafka.connect.s3.source;

/**
 * S3Offset is a representation of a consumer position for a partition backup on S3
 * @param filename the file name (object key) on S3 that is currently being consumed
 * @param offset the line number that is currently being consumed in a given file. Int.MAX_VALUE is used to represent the end of a file.
 */
public record S3Offset(String filename, long offset) { }
