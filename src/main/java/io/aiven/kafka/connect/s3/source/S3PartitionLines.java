package io.aiven.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Location;
import io.aiven.kafka.connect.s3.utils.StreamUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public final class S3PartitionLines {

     private record PrevCurrent(String prev, S3Location curr) {}

    public static Stream<RawSourceRecord> readLines(
            AmazonS3 client,
            S3Partition partition,
            FilenameParser parser,
            S3Offset offset,
            int maxFiles) {
        final var previousKey = offset == null ? null : offset.startAfterKey();

        final var lastProcessed = new AtomicReference<>(previousKey);
        return
                streamFiles(client, partition.bucket(), partition.prefix(), previousKey)
                        .limit(maxFiles)
                        .map(x -> new PrevCurrent(lastProcessed.getAndSet(x.getPrefix()), x))
                        .flatMap(x -> {
                            var source = parser.parse(x.curr.getPrefix());
                            try {
                                return sourceLines(client, source, x.prev, x.curr);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .skip(offset == null ? 0 : offset.offset());
    }

    private static Stream<RawSourceRecord> sourceLines(AmazonS3 client, SourceFile source, String previousKey, S3Location current)
            throws IOException {
        final var response = client.getObject(current.getBucketName(), current.getPrefix());

        var lineIterator = new LineIterator(new BufferedReader(new InputStreamReader(new GZIPInputStream(response.getObjectContent()))));
        var countedStream = StreamUtils.counting(1, lineIterator);

        return countedStream
                .map(line -> {
                    var offset = line.isLast()
                            ? new S3Offset(current.getPrefix(), 0)
                            : new S3Offset(previousKey, line.itemNumber());
                    return new RawSourceRecord(source, offset, line.item());
                });
    }

    private static Stream<S3Location> streamFiles(
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
