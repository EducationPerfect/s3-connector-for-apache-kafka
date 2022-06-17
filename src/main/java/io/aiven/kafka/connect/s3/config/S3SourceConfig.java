package io.aiven.kafka.connect.s3.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SourceConfig extends AivenCommonS3Config {
    /***
     * Configuration information:
     * `topic` - Destination topic to push to
     * `topic.source` - Source topic which is being pulled from
     * `topic.partition.id` - The partition id for which to process
     */

    public static final String GROUP_S3Config = "S3Config";

    public static final String TOPIC_SOURCE = "topic.source";
    public static final String TOPIC_PARTITION_ID = "topic.partition.id";

    public String getTopicSource() {
        return getString(TOPIC_SOURCE);
    }

    public int getPartitionId() {
        return getInt(TOPIC_PARTITION_ID);
    }

    public S3SourceConfig(Map<String, String> properties) {
        super(configDef(), properties);
    }

    public static ConfigDef configDef() {
        final var configDef = new S3SourceConfigDef();
        addAwsConfigGroup(configDef);
        addAwsStsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addOutputFieldsFormatConfigGroup(configDef, null);
        addDeprecatedTimestampConfig(configDef);
        addDeprecatedConfiguration(configDef);
        addKafkaBackoffPolicy(configDef);
        addS3RetryPolicies(configDef);
        addS3SourceConfigGroup(configDef);
        return configDef;
    }

    protected static void addS3SourceConfigGroup(final ConfigDef configDef) {
        int s3ConfigGroupCounter = 0;
        configDef.define(
                TOPIC_SOURCE,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "Source topic names",
                GROUP_S3Config,
                s3ConfigGroupCounter++,
                ConfigDef.Width.NONE,
                TOPIC_SOURCE
        );

    }
}
