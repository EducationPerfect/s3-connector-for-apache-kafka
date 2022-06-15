package io.aiven.kafka.connect.s3.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class S3SourceConfig extends AivenCommonS3Config {
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
        return configDef;
    }
}
