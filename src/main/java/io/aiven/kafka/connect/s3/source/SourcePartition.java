package io.aiven.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Template;

import java.util.*;
import org.apache.commons.io.*;

public final class SourcePartition {
    public static List<S3Partition> discoverPartitions(
            AmazonS3 client,
            String bucket,
            String fileNameTemplate,
            String[] topics) {
        String partitionClause = "(\\{\\{" + FilenameTemplateVariable.PARTITION.name + "(:\\d+?)?\\}\\})";
        String fullPrefix = FilenameUtils.getPath(fileNameTemplate);
        String partitionPrefix = fullPrefix.replaceAll(partitionClause + ".*$", "");
        String partitionTemplate = fullPrefix.replaceAll(partitionClause + ".*$", "$1");

        Template t = Template.of(partitionPrefix);
        FilenameParser parser = new FilenameParser(partitionTemplate);

        return Arrays.stream(topics)
                .flatMap(x -> {
                    String topicPrefix = t.instance().bindVariable(FilenameTemplateVariable.TOPIC.name, p -> x).render();
                    return listAllPrefixes(client, bucket, topicPrefix).stream();
                })
                .map(x -> new S3Partition(bucket, x))
                .toList();
    }

    private static Set<String> listAllPrefixes(AmazonS3 client, String bucket, String prefix) {
        Set<String> prefixes = new HashSet<>();

        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix)
                .withDelimiter("/");

        String continuationToken = null;
        do {
            ListObjectsV2Result result = client.listObjectsV2(request.withContinuationToken(continuationToken));
            prefixes.addAll(result.getCommonPrefixes());
            continuationToken = result.getNextContinuationToken();
        } while (continuationToken != null);

        return prefixes;
    }
}
