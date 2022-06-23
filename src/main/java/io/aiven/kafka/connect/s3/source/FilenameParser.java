package io.aiven.kafka.connect.s3.source;

import io.aiven.kafka.connect.common.config.FilenameTemplateVariable;

import java.nio.file.InvalidPathException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class FilenameParser {
    public final String fileNameTemplate;

    private static final Param topicParam = Param.of(FilenameTemplateVariable.TOPIC.name, ".+");
    private static final Param partitionParam = Param.of(FilenameTemplateVariable.PARTITION.name, "\\\\d+");
    private static final Param offsetParam = Param.of(FilenameTemplateVariable.START_OFFSET.name, "\\\\d+");

    private final Pattern parserRegex;

    public FilenameParser(String fileNameTemplate) {
        this.fileNameTemplate = fileNameTemplate;
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

    public SourceFileInfo parse(String fileName) {
        final Matcher matcher = parserRegex.matcher(fileName);
        if (matcher.find()) {
            return new SourceFileInfo(
                    fileName,
                    matcher.group(topicParam.groupName),
                    Integer.parseInt(matcher.group(partitionParam.groupName)),
                    getGroupOptional(matcher, offsetParam.groupName).map(Long::parseLong));
        }
        throw new InvalidPathException(fileName, "Unable to parse given file name. Regex: " + parserRegex.pattern());
    }

    private static Optional<String> getGroupOptional(Matcher matcher, String groupName) {
        try {
            return Optional.ofNullable(matcher.group(groupName));
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
    }

    static final class Param {
        private final String name;
        private final String groupName;
        private final String searchToken;
        private final String groupToken;
        private final String groupRefToken;

        public Param(String name, String groupName, String searchToken, String groupToken, String groupRefToken) {
            this.name = name;
            this.groupName = groupName;
            this.searchToken = searchToken;
            this.groupToken = groupToken;
            this.groupRefToken = groupRefToken;
        }

        public static Param of(String name, String expression) {
            final String groupName = name.replaceAll("[^a-zA-Z0-9]", "");
            return new Param(
                    name,
                    groupName,
                    "\\{\\{" + name + "(:.+?)?\\}\\}",
                    "(?<" + groupName + ">" + expression + ")",
                    "\\\\k<" + groupName + ">");
        }
    }
}
