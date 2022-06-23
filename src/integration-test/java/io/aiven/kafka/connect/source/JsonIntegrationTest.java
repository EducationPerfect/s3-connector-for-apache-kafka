package io.aiven.kafka.connect.source;

import cloud.localstack.Localstack;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.s3.AmazonS3;
import io.aiven.kafka.connect.ConnectRunner;
import io.aiven.kafka.connect.KafkaIntegrationBase;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector;
import io.aiven.kafka.connect.s3.AivenKafkaConnectS3SourceConnector;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = {"s3"})
@Testcontainers
public class JsonIntegrationTest implements KafkaIntegrationBase {
    private static final String S3_ACCESS_KEY_ID = "test-key-id0";
    private static final String S3_SECRET_ACCESS_KEY = "test_secret_key0";
    private static final String TEST_BUCKET_NAME = "test-bucket0";

    private static final String CONNECTOR_NAME = "aiven-s3-source-connector";
    private static final String SOURCE_TOPIC = "test-topic";
    private static final String TARGET_TOPIC = "test-topic-restored";

    private static String s3Endpoint;
    private static String s3Prefix;
    private static AmazonS3 s3Client;
    private static BucketAccessor testBucketAccessor;
    private static File pluginDir;

    private AdminClient adminClient;

    private ConnectRunner connectRunner;

    private static final int OFFSET_FLUSH_INTERVAL_MS = 5000;


    @Container
    private final KafkaContainer kafka = new KafkaContainer("5.2.1")
            .withExposedPorts(KafkaContainer.KAFKA_PORT, 9092)
            .withNetwork(Network.newNetwork())
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
//        s3Prefix = COMMON_PREFIX
//                + ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "/";

        s3Client = TestUtils.getClientS3();
        s3Endpoint = Localstack.INSTANCE.getEndpointS3();
        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET_NAME);
        testBucketAccessor.createBucket();

        pluginDir = KafkaIntegrationBase.getPluginDir();
        KafkaIntegrationBase.extractConnectorPlugin(pluginDir);
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        adminClient = newAdminClient(kafka);

        final NewTopic targetTopic = new NewTopic(TARGET_TOPIC, 4, (short) 1);
        adminClient.createTopics(List.of(targetTopic)).all().get();

        connectRunner = newConnectRunner(kafka, pluginDir, OFFSET_FLUSH_INTERVAL_MS);
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();

        connectRunner.awaitStop();
    }

    @Test
    public void should_start_connector() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = awsSpecificConfig(basicConnectorConfig(CONNECTOR_NAME));
        connectRunner.createConnector(Map.of());
    }

    private Map<String, String> basicConnectorConfig(final String connectorName) {
        final Map<String, String> config = new HashMap<>();
        config.put("name", connectorName);
        config.put("key.converter", "io.confluent.connect.avro.StringConverter");
        config.put("value.converter", "io.confluent.connect.avro.ByteArrayConverter");
        config.put("tasks.max", "1");
        return config;
    }


    private Map<String, String> awsSpecificConfig(final Map<String, String> config) {
        config.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        config.put("aws.access.key.id", S3_ACCESS_KEY_ID);
        config.put("aws.secret.access.key", S3_SECRET_ACCESS_KEY);
        config.put("aws.s3.endpoint", s3Endpoint);
        config.put("aws.s3.bucket.name", TEST_BUCKET_NAME);
        config.put("aws.s3.prefix", s3Prefix);
        config.put("topic.target", TARGET_TOPIC);
        config.put("tasks.max", "1");
        return config;
    }
}
