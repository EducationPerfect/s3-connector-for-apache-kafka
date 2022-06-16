package io.aiven.kafka.connect.s3;

import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import io.aiven.kafka.connect.common.templating.Template;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class S3SourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    private S3SourceConfig config;
    private AmazonS3 s3Client;
    private Map<S3Partition, S3Offset> offsets = new HashMap<>();
    protected AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        Objects.requireNonNull(props, "props hasn't been set");
        config = new S3SourceConfig(props);
        s3Client = createAmazonS3Client(config);

    }

    private AmazonS3 createAmazonS3Client(final S3SourceConfig config) {
        final var awsEndpointConfig = newEndpointConfiguration(this.config);
        final var clientConfig =
                PredefinedClientConfigurations.defaultConfig()
                        .withRetryPolicy(new RetryPolicy(
                                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                                new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
                                        Math.toIntExact(config.getS3RetryBackoffDelayMs()),
                                        Math.toIntExact(config.getS3RetryBackoffMaxDelayMs())
                                ),
                                config.getS3RetryBackoffMaxRetries(),
                                false)
                        );
        final var s3ClientBuilder =
                AmazonS3ClientBuilder
                        .standard()
                        .withCredentials(
                                credentialFactory.getProvider(config)
                        ).withClientConfiguration(clientConfig);
        if (Objects.isNull(awsEndpointConfig)) {
            s3ClientBuilder.withRegion(config.getAwsS3Region());
        } else {
            s3ClientBuilder.withEndpointConfiguration(awsEndpointConfig).withPathStyleAccessEnabled(true);
        }
        return s3ClientBuilder.build();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        var configBucket = config.getAwsS3BucketName();
        var configPrefix = config.getPrefixTemplate();

        LOGGER.info("Bucket name: {bucketName}, Prefix: {prefix}", new Object(){ final String bucketName = configBucket; final Template prefix = configPrefix; });

        return null;
    }

    @Override
    public void stop() {
        s3Client.shutdown();
        LOGGER.info("Stop S3 Source Task");
    }

    private AwsClientBuilder.EndpointConfiguration newEndpointConfiguration(final S3SourceConfig config) {
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return null;
        }
        return new AwsClientBuilder.EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName());
    }
}
