package no.ssb.dc.collection.api.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.Optional;

import static no.ssb.dc.collection.api.worker.CsvSpecification.*;

public class SpecificationDeserializer {

    private final JsonParser jsonParser;

    public SpecificationDeserializer() {
        jsonParser = JsonParser.createYamlParser();
    }

    public CsvSpecification parse(String data) {
        try {
            CsvSpecification.Builder builder = new CsvSpecification.Builder();

            ObjectMapper objectMapper = jsonParser.mapper();
            JsonNode root = objectMapper.readValue(data, JsonNode.class);
            String yaml = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            System.out.printf("yaml:%n%s%n", yaml);

            checkIfFieldExists(root, "backend");
            checkIfFieldExists(root, "metadata");
            checkIfFieldExists(root, "fileDescriptor");
            checkIfFieldExists(root, "keys");

            Optional.ofNullable(root.get("backend")).map(m -> builder.backend(BackendProvider.of(m.asText())));

            Metadata.Builder metadata = metadata();
            for (JsonNode node : root.withArray("metadata")) {
                Optional.ofNullable(getString(node, "source")).map(metadata::source);
                Optional.ofNullable(getString(node, "dataset")).map(metadata::dataset);
                Optional.ofNullable(getString(node, "tag")).map(metadata::tag);
                Optional.ofNullable(getString(node, "description")).map(metadata::description);
            }
            builder.metadata(metadata);

            FileDescriptor.Builder fileDescriptor = fileDescriptor();
            for (JsonNode node : root.withArray("fileDescriptor")) {
                Optional.ofNullable(getString(node, "delimiter")).map(m -> m.charAt(0)).map(fileDescriptor::delimiter);
                Optional.ofNullable(getString(node, "charset")).map(Charset::forName).map(fileDescriptor::charset);
                Optional.ofNullable(getString(node, "contentType")).map(fileDescriptor::contentType);
                Optional.ofNullable(getString(node, "files")).map(fileDescriptor::files);
            }
            builder.fileDescriptor(fileDescriptor);

            for (JsonNode node : root.withArray("keys")) {
                if (node.findParent("column") != null) {
                    ColumnKey.Builder columnKeys = column();
                    Optional.ofNullable(getString(node, "column")).map(columnKeys::name);
                    Optional.ofNullable(getString(node, "type")).map(type -> columnKeys.type(classForSimpleName(type)));
                    Optional.ofNullable(getString(node, "format")).map(columnKeys::format);
                    Optional.ofNullable(getString(node, "position")).map(position -> Boolean.parseBoolean(position) ? columnKeys.position() : false);
                    Optional.ofNullable(getString(node, "groupBy")).map(groupBy -> Boolean.parseBoolean(groupBy) ? columnKeys.groupBy() : false);
                    builder.columnKeys(columnKeys);

                } else if (node.findParent("function") != null) {
                    FunctionKey.Builder functionKeys = function();
                    Optional.ofNullable(getString(node, "function")).map(functionKeys::name);
                    Optional.ofNullable(getString(node, "generator")).map(generator -> functionKeys.generator(KeyGenerator.valueOf(generator)));
                    Optional.ofNullable(getString(node, "position")).map(position -> Boolean.parseBoolean(position) ? functionKeys.position() : false);
                    Optional.ofNullable(getString(node, "groupBy")).map(groupBy -> Boolean.parseBoolean(groupBy) ? functionKeys.groupBy() : false);
                    builder.columnKeys(functionKeys);

                } else {
                    throw new UnsupportedOperationException();
                }
            }

            return builder.build();

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void validate(CsvSpecification specification) {
        validateNotNull(specification.backend, "Backend must be defined");

        validateNotNull(specification.metadata.source, "Source files must be defined");
        validateNotNull(specification.metadata.tag, "Tag must be be defined");
        validateNotNull(specification.metadata.dataset, "Dataset must be be defined");

        validateNotNull(Character.toString(specification.fileDescriptor.delimiter), "Delimiter must be defined");
        validateNotNull(specification.fileDescriptor.charset, "Charset must be defined");
        validateNotNull(specification.fileDescriptor.contentType, "ContentType must be defined");
        validateNotNull(specification.fileDescriptor.files, "CSV files must be defined"); // TODO warn when csv files is not applied

        validateTrue(specification.columns.keys().isEmpty(), "CSV keys must be defined");
        validateTrue(specification.columns.positionKeys().isEmpty(), "Rawdata message position keys must be defined");
    }

    private void validateNotNull(Object target, String message) {
        if (target == null || "".equalsIgnoreCase(target.toString())) {
            throw new RuntimeException(message);
        }
    }

    private void validateTrue(boolean test, String message) {
        if (test) {
            throw new RuntimeException(message);
        }
    }

    private void checkIfFieldExists(JsonNode root, String fieldName) {
        if (!root.has(fieldName)) {
            throw new IllegalArgumentException("Validation error: missing '" + fieldName + "' definition");
        }
    }

    private Class<?> classForSimpleName(String simpleClassName) {
        if (simpleClassName.equalsIgnoreCase("String")) {
            return String.class;

        } else if (simpleClassName.equalsIgnoreCase("Long")) {
            return Long.class;

        } else if (simpleClassName.equalsIgnoreCase("Integer")) {
            return Integer.class;

        } else if (simpleClassName.equalsIgnoreCase("Date")) {
            return Date.class;

        } else {
            throw new UnsupportedOperationException("Simple class: " + simpleClassName + " is not supported!");
        }
    }

    String getString(JsonNode jsonNode, String fieldName) {
        return jsonNode.has(fieldName) ? jsonNode.get(fieldName).asText() : null;
    }
}
