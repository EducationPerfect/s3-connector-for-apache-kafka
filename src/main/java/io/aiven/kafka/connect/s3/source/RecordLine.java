package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;

/**
 * RecordLine represents a decoded record that is read from the backup file on S3
 * @param offset the original offset of the record
 * @param key the original key of the record
 * @param value the value of the record as a byte array
 * @param timestamp the original timestamp of the record
 * @param headers the original headers of the record
 */
public record RecordLine(Long offset, String key, byte[] value, DateTime timestamp, RecordHeader<?>[] headers) {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());

    public record RecordHeader<T>(String key, T value) { }

    public static RecordLine parseJson(String line) throws JsonProcessingException {
        return RecordLine.jsonMapper.readValue(line, RecordLine.class);
    }
}

