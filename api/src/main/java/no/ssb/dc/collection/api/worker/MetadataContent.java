package no.ssb.dc.collection.api.worker;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MetadataContent {

    private final ArrayNode elementsArray;

    public MetadataContent(ArrayNode elementsArray) {
        this.elementsArray = elementsArray;
    }

    public ArrayNode getElementNode() {
        return elementsArray;
    }

    public String toJSON() {
        return JsonParser.createJsonParser().toJSON(elementsArray);
    }

    public static class Builder {

        private JsonParser jsonParser = JsonParser.createJsonParser();
        private ObjectNode metadataNode = JsonParser.createJsonParser().createObjectNode();
        private ObjectNode mappingInfo = JsonParser.createJsonParser().createObjectNode();
        private ArrayNode mappingArray = JsonParser.createJsonParser().createArrayNode();

        public Builder topic(String topic) {
            metadataNode.put("topic", topic);
            return this;
        }

        public Builder position(String position) {
            metadataNode.put("position", position);
            return this;
        }

        public Builder contentKey(String contentKey) {
            metadataNode.put("content-key", contentKey);
            return this;
        }

        public Builder markCreatedDate() {
            String now = DateTimeFormatter.ISO_INSTANT.format(Instant.now().atOffset(ZoneOffset.UTC).toInstant());
            metadataNode.put("created-date", now);
            return this;
        }

        public Builder source(String source) {
            metadataNode.put("source", source);
            return this;
        }

        public Builder dataset(String dataset) {
            metadataNode.put("dataset", dataset);
            return this;
        }

        public Builder tag(String tag) {
            metadataNode.put("tag", tag);
            return this;
        }

        public Builder description(String description) {
            metadataNode.put("description", description);
            return this;
        }

        public Builder charset(String contentType) {
            metadataNode.put("charset", contentType);
            return this;
        }

        public Builder resourceType(String resourceType) {
            metadataNode.put("resource-type", resourceType);
            return this;
        }

        public Builder contentType(String contentType) {
            metadataNode.put("content-type", contentType);
            return this;
        }

        public Builder contentLength(int contentLength) {
            metadataNode.put("content-length", contentLength);
            return this;
        }

        public Builder sourcePath(String sourcePath) {
            mappingInfo.put("source-path", sourcePath);
            return this;
        }

        public Builder sourceFile(String sourceFile) {
            mappingInfo.put("source-file", sourceFile);
            return this;
        }

        public Builder recordType(String recordType) {
            mappingInfo.put("record-type", recordType);
            return this;
        }

        public Builder delimiter(String delimiterString) {
            mappingInfo.put("delimiter", delimiterString);
            return this;
        }

        public Builder sourceCharset(String charset) {
            mappingInfo.put("source-charset", charset);
            return this;
        }


        public Builder csvMapping(String csvColumn, String avroColumn) {
            ObjectNode mappingNode = jsonParser.createObjectNode();
            mappingNode.put("name", csvColumn);
            mappingNode.put("mapped-name", avroColumn);
            mappingNode.put("data-type", String.class.getSimpleName());
            mappingArray.add(mappingNode);
            return this;
        }

        public MetadataContent build() {
            ArrayNode elementsArray = jsonParser.createArrayNode();
            ObjectNode elementNode = jsonParser.createObjectNode();
            elementNode.set("metadata", metadataNode);
            mappingInfo.set("fields", mappingArray);
            elementNode.set("schema", mappingInfo);
            elementsArray.add(elementNode);
            return new MetadataContent(elementsArray);
        }
    }
}
