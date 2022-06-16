package io.aiven.kafka.connect.s3;

import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.aiven.kafka.connect.s3.config.AivenCommonS3Config;
import io.aiven.kafka.connect.s3.config.AwsCredentialProviderFactory;
import java.util.Objects;

public final class AWS {
    private static final AwsCredentialProviderFactory credentialFactory = new AwsCredentialProviderFactory();
    public static AmazonS3 createAmazonS3Client(final AivenCommonS3Config config) {
        final var awsEndpointConfig = newEndpointConfiguration(config);
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

    private static AwsClientBuilder.EndpointConfiguration newEndpointConfiguration(final AivenCommonS3Config config) {
        if (Objects.isNull(config.getAwsS3EndPoint())) {
            return null;
        }
        return new AwsClientBuilder.EndpointConfiguration(config.getAwsS3EndPoint(), config.getAwsS3Region().getName());
    }
}
