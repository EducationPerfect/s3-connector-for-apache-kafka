package io.aiven.kafka.connect.s3.source;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.aiven.kafka.connect.s3.S3SourceTask;
import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.testutils.BucketAccessor;
import io.findify.s3mock.S3Mock;
import org.apache.kafka.connect.runtime.WorkerSourceTaskContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.aiven.kafka.connect.s3.config.AivenCommonS3Config.*;

public final class S3SourceTaskTest {
    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;
    private static Map<String, String> commonProperties;

    private SourceTaskContext mockedSourceTaskContext;
    private OffsetBackingStore offsetBackingStore;

    private static BucketAccessor testBucketAccessor;

    @BeforeAll
    public static void setUpClass() {
        final Random generator = new Random();
        final int s3Port = generator.nextInt(10000) + 10000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(
                AWS_ACCESS_KEY_ID, "test_key_id",
                AWS_SECRET_ACCESS_KEY, "test_secret_key",
                AWS_S3_BUCKET, TEST_BUCKET,
                AWS_S3_ENDPOINT, "http://localhost:" + s3Port,
                AWS_S3_REGION, "us-west-2"
        );

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(AWS_ACCESS_KEY_ID),
                commonProperties.get(AWS_SECRET_ACCESS_KEY)
        );
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                commonProperties.get(AWS_S3_ENDPOINT),
                commonProperties.get(AWS_S3_REGION)
        ));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        offsetBackingStore = new MemoryOffsetBackingStore();
        var offsetReader = new OffsetStorageReaderImpl(offsetBackingStore, "test", new StringConverter(), new StringConverter());
        mockedSourceTaskContext = new WorkerSourceTaskContext(offsetReader);
        offsetBackingStore.start();
    }

    @AfterEach
    public void tearDown() {
        offsetBackingStore.stop();
//        s3Client.deleteBucket(TEST_BUCKET);
    }

    @Test
    public void foo() throws Exception {
        var sourceTask = new S3SourceTask();
        sourceTask.initialize(mockedSourceTaskContext);

        var partitionPrefixes = "/aiven/topic-a/partition-0/,/aiven/topic-a/partition-1/";

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIX, partitionPrefixes);

        sourceTask.start(taskProperties);

        var res = sourceTask.poll();

        assertNotNull(res);
    }
}
