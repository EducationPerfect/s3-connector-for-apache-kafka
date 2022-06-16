package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;

public final class JsonRecordParser {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());

    public record RecordHeader<T>(String key, T value) { }
    public record RecordLine(Long offset, String key, byte[] value, DateTime timestamp, RecordHeader<?>[] headers) { }

    public static RecordLine parse(String line)
            throws JsonProcessingException, JsonMappingException {
        return jsonMapper.readValue(line, RecordLine.class);
    }
}
