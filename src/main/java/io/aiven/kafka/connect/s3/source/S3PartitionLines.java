package io.aiven.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Location;
import io.aiven.kafka.connect.s3.utils.StreamUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class S3PartitionLines {
    public static Stream<RawRecordLine> readLines(
            AmazonS3 client,
            S3Partition partition,
            FilenameParser parser,
            S3Offset offset,
            int maxFiles) {
        final var previousKey = offset == null ? null : offset.filename();

        // If the current file is not yet process, start with it
        Stream<S3Location> prefixStream =
                (offset != null && previousKey != null && offset.offset() != Integer.MAX_VALUE)
                        ? Stream.of(new S3Location().withBucketName(partition.bucket()).withPrefix(previousKey))
                        : Stream.empty();

        var filesStream = Stream.concat(prefixStream, streamFiles(client, partition.bucket(), partition.prefix(), previousKey));

        return
                filesStream
                        .limit(maxFiles)
                        .flatMap(file -> {
                            var source = parser.parse(file.getPrefix());
                            return sourceLines(client, file)
                                    .map(line -> new RawRecordLine(source, line.getKey(), line.getValue()));

                        })
                        .skip(offset == null ? 0 : offset.offset());
    }

    private static Stream<Pair<S3Offset, String>> sourceLines(AmazonS3 client, S3Location current) {
        final var response = client.getObject(current.getBucketName(), current.getPrefix());

        try {
            var lineIterator = new LineIterator(new BufferedReader(new InputStreamReader(new GZIPInputStream(response.getObjectContent()))));
            var countedStream = StreamUtils.counting(1, lineIterator);

            return countedStream
                    .map(line -> {
                        var offset = line.isLast()
                                ? new S3Offset(current.getPrefix(), Integer.MAX_VALUE)
                                : new S3Offset(current.getPrefix(), line.itemNumber());
                        return Pair.of(offset, line.item());
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
