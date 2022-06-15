package io.aiven.kafka.connect.s3.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.List;
import java.util.Map;

public class S3SourceConfigDef extends ConfigDef  {
    @Override
    public List<ConfigValue> validate(final Map<String, String> props) {
        return super.validate(S3SourceConfig.preprocessProperties(props));
    }
}
