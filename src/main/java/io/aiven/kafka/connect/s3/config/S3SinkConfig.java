package io.aiven.kafka.connect.s3.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SinkConfig extends AivenCommonS3Config {
    public S3SinkConfig(Map<String, String> properties) {
        super(configDef(), properties);
    }

    public static ConfigDef configDef() {
        final var configDef = new S3SinkConfigDef();
        addAwsConfigGroup(configDef);
        addAwsStsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addOutputFieldsFormatConfigGroup(configDef, null);
        addDeprecatedTimestampConfig(configDef);
        addDeprecatedConfiguration(configDef);
        addKafkaBackoffPolicy(configDef);
        addS3RetryPolicies(configDef);
        return configDef;
    }
}
