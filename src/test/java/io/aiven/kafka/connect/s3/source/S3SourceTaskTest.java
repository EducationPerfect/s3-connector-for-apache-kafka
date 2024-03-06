package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.s3.config.S3SourceConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatCollection;
import static org.assertj.core.api.Assertions.assertThat;

public final class S3SourceTaskTest extends SourceTaskTestBase {

    @Test
    public void should_by_default_pause_between_polls_when_empty() throws Exception {
        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        sourceTask.start(taskProperties);

        var startTime = System.currentTimeMillis();
        var firstBatch = poll();
        var endTime = System.currentTimeMillis();

        var elapsedTime = endTime - startTime;
        assertThat(elapsedTime).isGreaterThanOrEqualTo(500);
        assertThat(firstBatch).isEmpty();
    }

    @Test
    public void should_respect_configured_between_polls_when_empty() throws Exception {
        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        taskProperties.put(S3SourceConfig.EMPTY_POLL_DELAY_MS, "10");
        sourceTask.start(taskProperties);

        var startTime = System.currentTimeMillis();
        var firstBatch = poll();
        var endTime = System.currentTimeMillis();

        var elapsedTime = endTime - startTime;
        assertThat(elapsedTime).isBetween(10L, 100L);
        assertThat(firstBatch).isEmpty();
    }

    @Test
    public void should_poll_multiple_partitions() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);
        createBackupFile(TEST_TOPIC, 0, 10L, 10);
        createBackupFile(TEST_TOPIC, 1, 0L, 13);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var firstBatch = poll();
        assertThatCollection(firstBatch).hasSize(20);
        assertThatCollection(firstBatch).allMatch(r -> r.kafkaPartition() == 0);

        // Expect data from 2nd partition
        var secondBatch = poll();
        assertThatCollection(secondBatch).hasSize(13);
        assertThatCollection(secondBatch).allMatch(r -> r.kafkaPartition() == 1);
    }

    @Test
    public void should_poll_partition_until_no_more_batches() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);
        createBackupFile(TEST_TOPIC, 0, 10L, 10);
        createBackupFile(TEST_TOPIC, 1, 0L, 13);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        taskProperties.put(S3SourceConfig.BATCH_SIZE_CONFIG, "15");

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var batch_1 = poll();
        assertThatCollection(batch_1).hasSize(15);
        assertThatCollection(batch_1).allMatch(r -> r.kafkaPartition() == 0);


        var batch_2 = poll();
        assertThatCollection(batch_2).hasSize(5);
        assertThatCollection(batch_2).allMatch(r -> r.kafkaPartition() == 0);

        // Expect data from 2nd partition
        var batch_3 = poll();
        assertThatCollection(batch_3).hasSize(13);
        assertThatCollection(batch_3).allMatch(r -> r.kafkaPartition() == 1);
    }

    @Test
    public void should_poll_partition_until_no_more_batches_for_batches_smaller_than_file() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);
        createBackupFile(TEST_TOPIC, 0, 10L, 3);
        createBackupFile(TEST_TOPIC, 1, 0L, 13);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        taskProperties.put(S3SourceConfig.BATCH_SIZE_CONFIG, "8");

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var batch_1 = poll();
        assertThatCollection(batch_1).hasSize(8);
        assertThatCollection(batch_1).allMatch(r -> r.kafkaPartition() == 0);


        var batch_2 = poll();
        assertThatCollection(batch_2).hasSize(5);
        assertThatCollection(batch_2).allMatch(r -> r.kafkaPartition() == 0);

        // Expect data from 2nd partition
        var batch_3 = poll();
        assertThatCollection(batch_3).hasSize(8);
        assertThatCollection(batch_3).allMatch(r -> r.kafkaPartition() == 1);

        var batch_4 = poll();
        assertThatCollection(batch_4).hasSize(5);
        assertThatCollection(batch_4).allMatch(r -> r.kafkaPartition() == 1);
    }

    @Test
    @Disabled("S3Mock doesn't support listing objects with 'withStartAfter' which if required for this test")
    public void should_poll_alterates_partitions() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);
        createBackupFile(TEST_TOPIC, 0, 10L, 10);
        createBackupFile(TEST_TOPIC, 1, 0L, 13);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/,aiven/%1$s/partition=1/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        taskProperties.put(S3SourceConfig.FILES_PAGE_SIZE_CONFIG, "1");

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var batch_1 = poll();
        assertThatCollection(batch_1).hasSize(10);
        assertThatCollection(batch_1).allMatch(r -> r.kafkaPartition() == 0);


        var batch_2 = poll();
        assertThatCollection(batch_2).hasSize(13);
        assertThatCollection(batch_2).allMatch(r -> r.kafkaPartition() == 1);

        // Expect data from 2nd partition
        var batch_3 = poll();
        assertThatCollection(batch_3).hasSize(10);
        assertThatCollection(batch_3).allMatch(r -> r.kafkaPartition() == 0);
    }

    @Test
    @Disabled("S3Mock doesn't support listing objects with 'withStartAfter' which if required for this test")
    public void should_keep_getting_records() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);

        sourceTask.start(taskProperties);

        // Expect data from 1st partition
        var batch_1 = poll();
        assertThatCollection(batch_1).hasSize(10);
        assertThatCollection(batch_1).allMatch(r -> r.kafkaPartition() == 0);


        var batch_2 = poll();
        assertThatCollection(batch_2).isEmpty();

        createBackupFile(TEST_TOPIC, 0, 10L, 10);

        // Expect data from 2nd partition
        var batch_3 = poll();
        assertThatCollection(batch_3).hasSize(10);
        assertThatCollection(batch_3).allMatch(r -> r.kafkaPartition() == 0);
    }

    @Test
    @Disabled("S3Mock doesn't support listing objects with 'withStartAfter' which if required for this test")
    public void should_continue_after_restart() throws Exception {
        createBackupFile(TEST_TOPIC, 0, 0L, 10);

        var partitionPrefixes = String.format("aiven/%1$s/partition=0/", TEST_TOPIC);

        var taskProperties = new HashMap<>(commonProperties);
        taskProperties.put(S3SourceConfig.PARTITION_PREFIXES_CONFIG, partitionPrefixes);
        taskProperties.put(S3SourceConfig.BATCH_SIZE_CONFIG, "8");

        sourceTask.start(taskProperties);

        var batch_1 = poll();
        assertThatCollection(batch_1).hasSize(8);

        // simulate crash and restart
        resetTask();
        sourceTask.start(taskProperties);

        var batch_2 = poll();
        assertThatCollection(batch_2).hasSize(2);
    }
}