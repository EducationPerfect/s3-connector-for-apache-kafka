package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public final class JsonRecordParserTest {
    private static final ObjectMapper permissiveJson = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void should_parse_record() {
        String line = """
                {"headers":[{"key":"eventId","value":"7f5140c3-1eae-4840-8b71-58ca2f6168af"},{"key":"streamType","value":"Vault"},{"key":"streamId","value":"eaae9ab6-f9c6-4240-a8a1-85aa38425fef"},{"key":"eventType","value":"EventSourcing.App.VaultInstalled"},{"key":"version","value":1},{"key":"timestamp","value":1655211964738}],"offset":236,"value":"eyJldmVudElkIjoiN2Y1MTQwYzMtMWVhZS00ODQwLThiNzEtNThjYTJmNjE2OGFmIiwiaGVhZGVycyI6eyJldmVudElkIjoiN2Y1MTQwYzMtMWVhZS00ODQwLThiNzEtNThjYTJmNjE2OGFmIiwic3RyZWFtVHlwZSI6IlZhdWx0Iiwic3RyZWFtSWQiOiJlYWFlOWFiNi1mOWM2LTQyNDAtYThhMS04NWFhMzg0MjVmZWYiLCJldmVudFR5cGUiOiJFdmVudFNvdXJjaW5nLkFwcC5WYXVsdEluc3RhbGxlZCIsInZlcnNpb24iOjEsInRpbWVzdGFtcCI6MTY1NTIxMTk2NDczOH0sInN0cmVhbVR5cGUiOiJWYXVsdCIsImRhdGEiOnsiZmFjaWxpdHlJZCI6IjUzOGU0Njc3LWYwYjAtNGRiNC1iZTdjLWJkZDZlNjAyYTBhMyJ9LCJzdHJlYW1JZCI6ImVhYWU5YWI2LWY5YzYtNDI0MC1hOGExLTg1YWEzODQyNWZlZiIsImV2ZW50VHlwZSI6IkV2ZW50U291cmNpbmcuQXBwLlZhdWx0SW5zdGFsbGVkIiwiaWQiOiJWYXVsdDplYWFlOWFiNi1mOWM2LTQyNDAtYThhMS04NWFhMzg0MjVmZWYiLCJ2ZXJzaW9uIjoxLCJ0aW1lc3RhbXAiOjE2NTUyMTE5NjQ3Mzh9","key":"eaae9ab6-f9c6-4240-a8a1-85aa38425fef","timestamp":"2022-06-14T13:06:05.065Z"}
                """;
        try {
            JsonRecordParser.RecordLine result = JsonRecordParser.parse(line);

            assertThat(result.offset()).isEqualTo(236);
            assertThat(result.key()).isEqualTo("eaae9ab6-f9c6-4240-a8a1-85aa38425fef");

            JsonNode payloadJson = permissiveJson.readTree(new String(result.value()));

//            JsonNode headers = payloadJson.get("headers");
//
//            JsonRecordParser.RecordHeader<?>[] headers = permissiveJson.convertValue(foo.get("headers"), (JsonRecordParser.RecordHeader[].class));
//
//            assertThat(result.headers()).isEqualTo(headers);

        } catch (JsonProcessingException e) {
            fail(e.getMessage());
        }
    }
}
