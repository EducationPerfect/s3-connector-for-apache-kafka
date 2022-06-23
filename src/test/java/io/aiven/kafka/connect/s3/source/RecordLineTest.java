package io.aiven.kafka.connect.s3.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.*;
import net.jqwik.time.api.DateTimes;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import scala.Array;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.*;

public final class RecordLineTest {
    private static final ObjectMapper permissiveJson = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void should_parse_record() throws Exception {
        String line = """
                {"headers":[{"key":"eventId","value":"7f5140c3-1eae-4840-8b71-58ca2f6168af"},{"key":"streamType","value":"Vault"},{"key":"streamId","value":"eaae9ab6-f9c6-4240-a8a1-85aa38425fef"},{"key":"eventType","value":"EventSourcing.App.VaultInstalled"},{"key":"version","value":1},{"key":"timestamp","value":1655211964738}],"offset":236,"value":"eyJldmVudElkIjoiN2Y1MTQwYzMtMWVhZS00ODQwLThiNzEtNThjYTJmNjE2OGFmIiwiaGVhZGVycyI6eyJldmVudElkIjoiN2Y1MTQwYzMtMWVhZS00ODQwLThiNzEtNThjYTJmNjE2OGFmIiwic3RyZWFtVHlwZSI6IlZhdWx0Iiwic3RyZWFtSWQiOiJlYWFlOWFiNi1mOWM2LTQyNDAtYThhMS04NWFhMzg0MjVmZWYiLCJldmVudFR5cGUiOiJFdmVudFNvdXJjaW5nLkFwcC5WYXVsdEluc3RhbGxlZCIsInZlcnNpb24iOjEsInRpbWVzdGFtcCI6MTY1NTIxMTk2NDczOH0sInN0cmVhbVR5cGUiOiJWYXVsdCIsImRhdGEiOnsiZmFjaWxpdHlJZCI6IjUzOGU0Njc3LWYwYjAtNGRiNC1iZTdjLWJkZDZlNjAyYTBhMyJ9LCJzdHJlYW1JZCI6ImVhYWU5YWI2LWY5YzYtNDI0MC1hOGExLTg1YWEzODQyNWZlZiIsImV2ZW50VHlwZSI6IkV2ZW50U291cmNpbmcuQXBwLlZhdWx0SW5zdGFsbGVkIiwiaWQiOiJWYXVsdDplYWFlOWFiNi1mOWM2LTQyNDAtYThhMS04NWFhMzg0MjVmZWYiLCJ2ZXJzaW9uIjoxLCJ0aW1lc3RhbXAiOjE2NTUyMTE5NjQ3Mzh9","key":"eaae9ab6-f9c6-4240-a8a1-85aa38425fef","timestamp":"2022-06-14T13:06:05.065Z"}
                """;
        var result = RecordLine.parseJson(line);

        assertThat(result.offset()).isEqualTo(236);
        assertThat(result.key()).isEqualTo("eaae9ab6-f9c6-4240-a8a1-85aa38425fef");

        // Now check if the payload is decoded correctly
        JsonNode payloadJson = permissiveJson.readTree(new String(result.value()));
        assertThat(payloadJson.has("headers")).isTrue();
        assertThat(payloadJson.has("data")).isTrue();
    }

    @Provide
    Arbitrary<RecordLine> arbitraryRecord() {
        var headers =
                Combinators.combine(
                                Arbitraries.strings(),  // key
                                Arbitraries.oneOf(      // different possible values
                                        Arbitraries.strings(),
                                        Arbitraries.integers(),
                                        Arbitraries.defaultFor(Boolean.class)))
                        .as(RecordLine.RecordHeader::new)
                        .array(RecordLine.RecordHeader[].class);

        var utcDatetime = DateTimes
                .instants()
                .map(x -> new DateTime(x.getEpochSecond(), DateTimeZone.UTC));

        return Combinators.combine(
                        Arbitraries.longs(),
                        Arbitraries.strings(),
                        Arbitraries.defaultFor(byte[].class),
                        utcDatetime,
                        headers
                )
                .as(RecordLine::new);
    }

    @Property
    void should_roundtrip_encoding(@ForAll("arbitraryRecord") RecordLine line) throws Exception {
        var encoded = RecordLine.encodeJson(line);
        var decoded = RecordLine.parseJson(encoded);

        assertEquals(line, decoded);
    }
}
