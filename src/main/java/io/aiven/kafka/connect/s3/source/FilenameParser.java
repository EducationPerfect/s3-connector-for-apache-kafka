package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilenameParser {
    private static final Param topicParam = Param.of(FilenameTemplateVariable.TOPIC.name);
    private static final Param partitionParam = Param.of(FilenameTemplateVariable.PARTITION.name);
    private static final Param offsetParam = Param.of(FilenameTemplateVariable.START_OFFSET.name);

    private final Pattern parserRegex;

    public record ParserResult(String topic, int partition, long offset) {
    }

    ;

    private record Param(String name, String groupName, String searchToken, String groupToken, String groupRefToken) {
        public static Param of(String name) {
            final String groupName = name.replaceAll("[^a-zA-Z0-9]", "");
            return new Param(
                    name,
                    groupName,
                    "\\{\\{" + name + "(:.+?)?\\}\\}",
                    "(?<" + groupName + ">.+)",
                    "\\\\k<" + groupName + ">");
        }
    }

    public FilenameParser(String fileNameTemplate) {
        final Param[] params = {topicParam, partitionParam, offsetParam};
        String result = fileNameTemplate.replaceAll("[-\\[\\]()*+?.,\\\\\\\\^$|#]", "\\\\$0");

        for (Param param : params) {
            result = result
                    .replaceFirst(param.searchToken, param.groupToken)
                    .replaceAll(param.searchToken, param.groupRefToken);
        }

        final String regex = result.replaceAll("\\{\\{.+?\\}\\}", ".+?");

        parserRegex = Pattern.compile(regex);
    }

    public ParserResult parse(String fileName) {
        final Matcher matcher = parserRegex.matcher(fileName);
        if (matcher.matches()) {
            try {
                return new ParserResult(matcher.group(topicParam.groupName),
                        Integer.parseInt(matcher.group(partitionParam.groupName)),
                        Long.parseLong(matcher.group(offsetParam.groupName)));
            } catch (Exception ex) {
                return null;
            }
        }
        return null;
    }
}
