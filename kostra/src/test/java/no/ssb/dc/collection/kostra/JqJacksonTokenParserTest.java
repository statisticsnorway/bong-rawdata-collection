package no.ssb.dc.collection.kostra;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JqJacksonTokenParserTest {

    private static final Logger LOG = LoggerFactory.getLogger(JqJacksonTokenParserTest.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void jqQuery() throws IOException {
        Path source = Paths.get(".").normalize().resolve(Paths.get("src/test/resources/data/kostradata.json"));
        JsonNode root = mapper.readTree(source.toFile());
        JsonQuery query = JsonQuery.compile(".structure", Version.LATEST);
        AtomicReference<JsonNode> resultRef = new AtomicReference<>();

        query.apply(Scope.newEmptyScope(), root, resultRef::set);

        LOG.trace("{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(resultRef.get()));
    }

    @Disabled
    @Test
    void tokenParserWithJq() throws IOException {
        Function<Integer, String> indent = i -> Arrays.stream(new String[i]).map(element -> " ").collect(Collectors.joining());

        Path source = Paths.get(".").normalize().resolve(Paths.get("src/test/resources/data/kostradata.json"));
        LOG.info("Parse file {} with {} encoding", source.normalize().toAbsolutePath().toString(), StandardCharsets.UTF_8);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source.toFile()), StandardCharsets.UTF_8))) {
            try (JsonParser parser = mapper.createParser(reader)) {
                LOG.trace("{}", parser);

                JsonToken jsonToken;

                AtomicReference<ContainerNode<?>> rootNodeRef = new AtomicReference<>();

                Deque<ContainerNode<?>> stack = new ArrayDeque<>();
                Deque<Map.Entry<JsonToken, String>> fieldNameStack = new ArrayDeque<>();

                while ((jsonToken  = parser.nextToken()) != null) {
                    LOG.info("loc: {}, {}", parser.getParsingContext().getParent().pathAsPointer(), parser.getParsingContext().getParent().inArray());
                    // handle root
                    if (rootNodeRef.get() == null && jsonToken.isStructStart()) {
                        ContainerNode<?> startJsonOrArrayNode = jsonToken == JsonToken.START_OBJECT ? mapper.createObjectNode() : mapper.createArrayNode();
                        rootNodeRef.compareAndSet(null, startJsonOrArrayNode);
                        stack.add(startJsonOrArrayNode);
                    }

                    // position to previous sibling or parent
                    if (jsonToken.isStructEnd()) {
                        stack.pollLast();
                    }

                    // mark last known field name
                    if (jsonToken == JsonToken.FIELD_NAME) {
                        fieldNameStack.add(Map.entry(jsonToken, parser.getCurrentName()));
                    }


                    if (!fieldNameStack.isEmpty() && fieldNameStack.peek().getKey() == JsonToken.FIELD_NAME && jsonToken.isStructStart()) {
                        ObjectNode currentObjectNode = (ObjectNode) stack.peek();
                        if (jsonToken == JsonToken.START_OBJECT) {
                            ObjectNode newObjectNode = currentObjectNode.putObject(fieldNameStack.peek().getValue());
                            stack.add(newObjectNode);
                        }
                        if (jsonToken == JsonToken.START_ARRAY) {
                            ArrayNode newArrayNode = currentObjectNode.putArray(fieldNameStack.peek().getValue());
                            stack.add(newArrayNode);
                        }
                    }

                    if (!fieldNameStack.isEmpty() && fieldNameStack.peek().getKey() == JsonToken.FIELD_NAME && jsonToken.isScalarValue()) {
                        ContainerNode<?> currentNode = stack.peek();
                        if (currentNode.isObject()) {
                            ((ObjectNode)currentNode).put(fieldNameStack.peek().getValue(), parser.getValueAsString());
                            //LOG.trace("field: {}", currentNode.toString());
                        }
                        if (currentNode.isArray()) {
                            ((ArrayNode)currentNode).add(parser.getValueAsString());
                            //LOG.trace("field: {}", currentNode.toString());
                        }
                        fieldNameStack.pollLast();
                    }

                    //LOG.trace("{}{}", indent.apply(stack.size()), String.format("%-2s%-20s%-40s%s", stack.size(), jsonToken, parser.getCurrentName(), parser.getValueAsString()));
                }

                LOG.trace("stack: {}", stack.size());
                LOG.trace("json:\n{}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNodeRef.get()));

            }
        }
    }

    @Test
    void tokenParserWithPath() throws IOException {
        Function<Integer, String> indent = i -> Arrays.stream(new String[i]).map(element -> " ").collect(Collectors.joining());

        Path source = Paths.get(".").normalize().resolve(Paths.get("src/test/resources/data/kostradata.json"));
        LOG.info("Parse file {} with {} encoding", source.normalize().toAbsolutePath().toString(), StandardCharsets.UTF_8);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source.toFile()), StandardCharsets.UTF_8))) {
            try (JsonParser parser = mapper.createParser(reader)) {
                JsonToken jsonToken;
                while ((jsonToken  = parser.nextToken()) != null) {
                    if (!jsonToken.isScalarValue()) {
                        PathMatcher pathMatcher = new PathMatcher(parser.getParsingContext(), jsonToken);
                        LOG.info("{} -- {} -- {} -- {} -- {}", parser.getParsingContext().pathAsPointer().toString(),
                                pathMatcher.toExpr(),
                                pathMatcher.isMatch("/structure"),
                                pathMatcher.isMatch("/data[][]"),
                                pathMatcher.pathElements.peekLast().token
                        );

                        if (pathMatcher.isMatch("/structure[]")) {
                            TreeNode val = mapper.readTree(parser);
                            LOG.trace("{}", val);
                        }
                        if (pathMatcher.isMatch("/data[][]")) {
                            TreeNode val = mapper.readTree(parser);
                            LOG.trace("{}", val);
                        }
                    }
                }
            }
        }
    }

    enum TokenType {
        ROOT,
        OBJECT,
        ARRAY;
    }

    static class PathMatcher {
        final Deque<PathInfo> pathElements;

        PathMatcher(JsonStreamContext parsingContext, JsonToken jsonToken) {
            pathElements = new ArrayDeque<>();
            while (parsingContext != null) {
                TokenType type = parsingContext.inRoot() ? TokenType.ROOT : parsingContext.inObject() ? TokenType.OBJECT : TokenType.ARRAY;
                pathElements.addFirst(new PathInfo(parsingContext.pathAsPointer().toString(), parsingContext.getCurrentName(), type, jsonToken));
                parsingContext = parsingContext.getParent();
            }
        }

        PathMatcher(Deque<PathInfo> pathElements) {
            this.pathElements = pathElements;
        }

        /**
         *
         *
         * @param expr
         * @return
         */
        boolean isMatch(String expr) {
            return expr.equals(toExpr());
        }

        static boolean match(Deque<PathInfo> pathElements, String expr) {
            return new PathMatcher(pathElements).isMatch(expr);
        }

        public String toExpr() {
            StringBuilder builder = new StringBuilder();
            List<PathInfo> elements = pathElements.stream().filter(path -> !path.token.isScalarValue()).collect(Collectors.toList());
            for (PathInfo pathInfo : elements) {
                builder.append(switch (pathInfo.type) {
                    case ROOT -> "";
                    case OBJECT -> pathInfo.name == null ? "" : "/" + pathInfo.name;
                    case ARRAY -> "[]";
                });
            }
            return builder.toString();
        }

        public String toPathElementsAsString() {
            return "[" + pathElements.stream().map(PathInfo::toString).collect(Collectors.joining(", ")) + "]";
        }
    }

    static class PathInfo {
        final String path;
        final String name;
        final TokenType type;
        final JsonToken token;

        PathInfo(String path, String name, TokenType type, JsonToken token) {
            this.path = path;
            this.name = name;
            this.type = type;
            this.token = token;
        }

        @Override
        public String toString() {
            return String.format("{\"path\": \"%s\", \"name\": \"%s\", \"type\": \"%s\", \"token\": \"%s\"}", path, name, type.name().toLowerCase(), token);
        }
    }
}
