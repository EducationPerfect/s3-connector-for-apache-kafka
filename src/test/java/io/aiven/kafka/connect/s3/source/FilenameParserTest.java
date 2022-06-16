package io.aiven.kafka.connect.s3.source;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.NotBlank;
import net.jqwik.api.constraints.Positive;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

public class FilenameParserTest {

    @Property
    void should_parse_file_name(
            @ForAll @NotBlank @AlphaChars String topic,
            @ForAll @Positive int partition,
            @ForAll @Positive long offset) {
        String fileName = String.format("aiven/%1$s/partition=%2$d/%1$s+%2$d+%3$d.json.gz", topic, partition, offset);
        String fileNameTemplate = "aiven/{{topic}}/partition={{partition}}/{{topic}}+{{partition}}+{{start_offset:padding=true}}.json.gz";

        FilenameParser parser = new FilenameParser(fileNameTemplate);
        FilenameParser.ParserResult result = parser.parse(fileName);

        assertThat(result).isEqualTo(new FilenameParser.ParserResult(topic, partition, Optional.of(offset)));
    }
}
