package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;

import java.io.IOException;

public final class JsonRecordParser {
    private static final ObjectMapper jsonMapper = new ObjectMapper().registerModule(new JodaModule());

//    public interface IRecordHeader {
//        String getKey();
//        Object getValue();
//    }
//
//    private record RecordHeader<T>(String key, T value) implements IRecordHeader {
//        @Override
//        public String getKey() {
//            return key;
//        }
//
//        @Override
//        public Object getValue() {
//            return value;
//        }
//    }

//    private class HeaderDeserializer extends StdDeserializer<RecordHeader> {
//
//        protected HeaderDeserializer(Class<?> vc) {
//            super(vc);
//        }
//
//        @Override
//        public RecordHeader deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
//
//            return null;
//        }
//    }

    public record RecordHeader<T>(String key, T value) { }

    public record RecordLine(Long offset, String key, byte[] value, DateTime timestamp, RecordHeader<?>[] headers) { }

    public static RecordLine parse(String line)
            throws JsonProcessingException, JsonMappingException {
        return jsonMapper.readValue(line, RecordLine.class);
    }
}
