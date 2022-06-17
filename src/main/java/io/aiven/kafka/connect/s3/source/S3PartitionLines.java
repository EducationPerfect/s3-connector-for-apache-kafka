package io.aiven.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Location;
import io.aiven.kafka.connect.s3.S3Offset;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class S3PartitionLines {

    public record Line(S3Offset offset, String line) {
    }

    public static Stream<Line> readLines(AmazonS3 client, String bucket, String prefix, String afterKey) {
        return
                streamFiles(client, bucket, prefix, afterKey)
                        .flatMap(x -> {
                            try {
                                return lines(client, x);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    public static Stream<Line> lines(AmazonS3 client, S3Location object)
            throws IOException {
        final var response = client.getObject(object.getBucketName(), object.getPrefix());

        final var lineNumber = new AtomicLong();
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(response.getObjectContent())))
                .lines()
                .map(line -> new Line(new S3Offset(object.getPrefix(), lineNumber.getAndIncrement()), line))
                .onClose(() -> {
                    try {
                        response.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public static Stream<S3Location> streamFiles(
            AmazonS3 client,
            String bucket,
            String prefix,
            String afterKey) {

        final var request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix)
                .withDelimiter("/")
                .withMaxKeys(20)
                .withStartAfter(afterKey);

        var response = client.listObjectsV2(request);

        return Stream
                .iterate(response,
                        Objects::nonNull,
                        r -> r.getNextContinuationToken() == null ? null : client.listObjectsV2(request.withContinuationToken(r.getNextContinuationToken()))
                )
                .flatMap(r -> r.getObjectSummaries().stream())
                .map(r -> new S3Location().withBucketName(bucket).withPrefix(r.getKey()));

    }
}
