package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.S3Partition;
import io.aiven.kafka.connect.s3.source.SourcePartition;
import io.aiven.kafka.connect.s3.utils.StreamUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AivenKafkaConnectS3SourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(AivenKafkaConnectS3SourceConnector.class);

    private Map<String, String> configProperties;

    @Override
    public ConfigDef config() { return S3SourceConfig.configDef(); }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return S3SourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks == 0) {
            return new ArrayList<>();
        }

        var config = new S3SourceConfig(Map.copyOf(configProperties));

        AmazonS3 client = AWS.createAmazonS3Client(config);
        String bucket = config.getAwsS3BucketName();
        String filenameTemplate = config.getFilenameTemplate().toString();
        String[] sourceTopics = config.getTopicSource().split(",");

        List<S3Partition> partitions = SourcePartition.discoverPartitions(client, bucket, filenameTemplate, sourceTopics);

        int batchSize = (int) Math.ceil((double) partitions.size() / maxTasks);

        var prefixes = partitions.stream().map(S3Partition::prefix);
        return StreamUtils
                .batching(batchSize, prefixes)
                .map(xs -> String.join(",", xs))
                .map(x -> {
                    Map<String, String> cfg = new HashMap<>(configProperties);
                    cfg.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, x);
                    return cfg;
                })
                .collect(Collectors.toList());

    }

    @Override
    public void start(final Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties haven't been set");
        configProperties = Map.copyOf(properties);
        LOGGER.info("Start S3 Source connector");
    }

    @Override
    public void stop() {
        LOGGER.info("Stop S3 Source connector");
    }
}
