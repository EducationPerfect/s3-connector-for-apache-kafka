package io.aiven.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Location;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class S3PartitionLines {

    public record Line(SourceDataInfo source, S3Offset offset, String line) {
    }

    private record PrevCurrent(String prev, S3Location curr) {}

    public static Stream<Line> readLines(AmazonS3 client, FilenameParser parser, S3Partition partition, S3Offset offset) {
        final var previousKey = offset == null ? null : offset.lastFullyProcessedKey();

        final var lastProcessed = new AtomicReference<>(previousKey);
        return
                streamFiles(client, partition.bucket(), partition.prefix(), previousKey)
                        .map(x -> new PrevCurrent(lastProcessed.getAndSet(x.getPrefix()), x))
                        .flatMap(x -> {
                            var source = parser.parse(x.curr.getPrefix());
                            try {
                                return lines(client, source, x.prev, x.curr);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .skip(offset == null ? 0 : offset.offset());
    }

    public static Stream<Line> lines(AmazonS3 client, SourceDataInfo source, String previousKey, S3Location current)
            throws IOException {
        final var response = client.getObject(current.getBucketName(), current.getPrefix());

        final var lineNumber = new AtomicLong();
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(response.getObjectContent())))
                .lines()
                .map(line -> new Line(source, new S3Offset(previousKey, current.getPrefix(), lineNumber.getAndIncrement()), line))
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
