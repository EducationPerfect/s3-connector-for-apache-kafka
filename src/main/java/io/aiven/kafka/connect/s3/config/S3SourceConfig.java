package io.aiven.kafka.connect.s3.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SourceConfig extends AivenCommonS3Config {
    /***
     * Configuration information:
     * `topic.target` - Destination topic to push to
     * `topic.source` - Source topic which is being pulled from
     * `partition.prefixes` - Prefixes for S3 files in a given partition. comma separated.
     */

    public static final String BATCH_SIZE_CONFIG = "batch.size";

    public static final String EMPTY_POLL_DELAY_MS = "poll.delay.empty.ms";

    public static final String FILES_PAGE_SIZE_CONFIG = "files.page.size";

    public static final String GROUP_S3Config = "S3Config";
    public static final String GROUP_Connector = "Connector";

    public static final String TOPIC_SOURCE_CONFIG = "topic.source";
    public static final String PARTITION_PREFIXES_CONFIG = "partition.prefixes";
    public static final String TOPIC_TARGET_CONFIG = "topic.target";

    public String getTopicSource() {
        return getString(TOPIC_SOURCE_CONFIG);
    }

    public String getTopic() {
        return getString(TOPIC_TARGET_CONFIG);
    }

    public String[] getPartitionPrefixes() {
        return getString(PARTITION_PREFIXES_CONFIG).split(",");
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public int getEmptyPollDelayMs() { return getInt(EMPTY_POLL_DELAY_MS); }

    public int getFilesPageSize() {
        return getInt(FILES_PAGE_SIZE_CONFIG);
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
        addConnectorConfiguration(configDef);
        return configDef;
    }

    protected static void addConnectorConfiguration(final ConfigDef configDef) {
        int groupOrder = 0;
        configDef.define(
                TOPIC_TARGET_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "Topic to push to",
                GROUP_Connector,
                groupOrder++,
                ConfigDef.Width.NONE,
                TOPIC_SOURCE_CONFIG);

        configDef.define(
                BATCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                1000,
                ConfigDef.Range.between(1, 20000),
                ConfigDef.Importance.HIGH,
                "The number of records to process when writing to Kafka in a single batch.",
                GROUP_Connector,
                groupOrder++,
                ConfigDef.Width.NONE,
                BATCH_SIZE_CONFIG);

        configDef.define(
                FILES_PAGE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                10,
                ConfigDef.Range.between(1, 200),
                ConfigDef.Importance.MEDIUM,
                "The number of files per partition to process as one iteration before moving to other partitions.",
                GROUP_Connector,
                groupOrder++,
                ConfigDef.Width.NONE,
                FILES_PAGE_SIZE_CONFIG);

        configDef.define(
                EMPTY_POLL_DELAY_MS,
                ConfigDef.Type.INT,
                500,
                ConfigDef.Range.between(0, 600000),
                ConfigDef.Importance.MEDIUM,
                "The amount of time (in milliseconds) to wait before polling for more messages when the previous batch returned no results",
                GROUP_Connector,
                groupOrder++,
                ConfigDef.Width.NONE,
                EMPTY_POLL_DELAY_MS);
    }

    protected static void addS3SourceConfigGroup(final ConfigDef configDef) {
        int s3ConfigGroupCounter = 0;
        configDef.define(
                TOPIC_SOURCE_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "Source topic names",
                GROUP_S3Config,
                s3ConfigGroupCounter++,
                ConfigDef.Width.NONE,
                TOPIC_SOURCE_CONFIG
        );

        configDef.define(
                PARTITION_PREFIXES_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "S3 file name prefixes for the partitions to work on. (S3 folders)",
                GROUP_S3Config,
                s3ConfigGroupCounter++,
                ConfigDef.Width.NONE,
                PARTITION_PREFIXES_CONFIG
        );

    }
}
