package io.aiven.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3SourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    private S3SourceConfig config;
    private AmazonS3 s3Client;
    private Map<S3Partition, S3Offset> offsets = new HashMap<>();
    protected AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }

    private static String templateExp(String name) {
        return "\\{\\{" + name + "(:.+?)?\\}\\}";
    }

    public static String buildRegex(String fileNameTemplate) {
        final String topicNameParam = templateExp(FilenameTemplateVariable.TOPIC.name);
        final String partitionParam = templateExp(FilenameTemplateVariable.PARTITION.name);
        final String offsetParam = templateExp(FilenameTemplateVariable.START_OFFSET.name);

        return fileNameTemplate
                // make it regex friendly, but keep {} as they are
                .replaceAll("[-\\[\\]()*+?.,\\\\\\\\^$|#]", "\\\\$0")

                // turn each of the interesting variables into a regex group
                .replaceFirst(topicNameParam, "(?<topic>.+)")
                .replaceFirst(partitionParam, "(?<partition>\\\\d+)")
                .replaceFirst(offsetParam, "(?<offset>\\\\d+)")

                // replace all the uninteresting template variables with a wildcard
                .replaceAll("\\{\\{.+?\\}\\}", ".+?");
    }
}
