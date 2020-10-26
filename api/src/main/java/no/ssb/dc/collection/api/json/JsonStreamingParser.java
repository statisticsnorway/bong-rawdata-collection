package no.ssb.dc.collection.api.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class JsonStreamingParser {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Path filename;

    public JsonStreamingParser(BufferedReader reader, String filepath, List<String> files, String filename) {
        this.filename = Paths.get(filename);
    }

    public void parse() {
        try {
            JsonFactory jsonfactory = new JsonFactory();
            try (FileInputStream fis = new FileInputStream(filename.toFile())) {
                try (com.fasterxml.jackson.core.JsonParser parser = jsonfactory.createParser(fis)) {
                    int n = 0;
                    if (parser.nextToken() != JsonToken.START_ARRAY) {
                        throw new IllegalStateException("Array node NOT found!");
                    }

                    while (parser.nextToken() == JsonToken.START_OBJECT) {
                        JsonNode jsonNode = OBJECT_MAPPER.readValue(parser, JsonNode.class);
                        System.out.printf("%s%n", jsonNode);

                        if (n == 150) break;
                        n++;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
