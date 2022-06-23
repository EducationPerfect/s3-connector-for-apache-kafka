package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Objects;

/**
 * RecordLine represents a decoded record that is read from the backup file on S3
 */

public final class RecordLine {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());

    @JsonProperty("offset") private final Long offset;
    @JsonProperty("key") private final String key;
    @JsonProperty("value") private final byte[] value;
    @JsonProperty("timestamp") private final DateTime timestamp;
    @JsonProperty("headers") private final RecordHeader<?>[] headers;

    /**
     * @param offset the original offset of the record
     * @param key the original key of the record
     * @param value the value of the record as a byte array
     * @param timestamp the original timestamp of the record
     * @param headers the original headers of the record
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RecordLine(
            @JsonProperty("offset") Long offset,
            @JsonProperty("key") String key,
            @JsonProperty("value") byte[] value,
            @JsonProperty("timestamp") DateTime timestamp,
            @JsonProperty("headers") RecordHeader<?>[] headers) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public Long offset() {
        return offset;
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public DateTime timestamp() {
        return timestamp;
    }

    public RecordHeader<?>[] headers() {
        return headers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RecordLine) obj;
        return Objects.equals(this.offset, that.offset) &&
                Objects.equals(this.key, that.key) &&
                Arrays.equals(this.value, that.value) &&
                Objects.equals(this.timestamp, that.timestamp) &&
                Arrays.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, key, value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "RecordLine[" +
                "offset=" + offset + ", " +
                "key=" + key + ", " +
                "value=" + value + ", " +
                "timestamp=" + timestamp + ", " +
                "headers=" + headers + ']';
    }

    static public final class RecordHeader<T> {
        @JsonProperty("key") public final String key;
        @JsonProperty("value") public final T value;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public RecordHeader(@JsonProperty("key") String key, @JsonProperty("value") T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (RecordHeader) obj;
            return Objects.equals(this.key, that.key) &&
                    Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "RecordHeader[" +
                    "key=" + key + ", " +
                    "value=" + value + ']';
        }

    }

    public static RecordLine parseJson(String line) throws JsonProcessingException {
        return RecordLine.jsonMapper.readValue(line, RecordLine.class);
    }

    public static String encodeJson(RecordLine record) throws JsonProcessingException {
        return RecordLine.jsonMapper.writeValueAsString(record);
    }
}

