package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class S3SourceTaskTest extends SourceTaskTestBase {

    @Test
    public void should_poll_multiple_partitions() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);
        createBackupFile(TEST_TOPIC, 0, 10L, 10);
        createBackupFile(TEST_TOPIC, 1, 0L, 13);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIX, partitionPrefixes);

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var firstPartition = sourceTask.poll();
        assertEquals(20, firstPartition.size());

        // Expect data from 2nd partition
        var secondPartition = sourceTask.poll();
        assertEquals(13, secondPartition.size());
    }
}