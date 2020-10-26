package no.ssb.dc.collection.api.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class JsonStreamingParserTest {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void parseLargeFile() throws IOException {
        String largeJsonFile = "/Users/oranheim/IdeaProjects/ssb/rawdata-collection-client/api/src/test/data/enheter_alle.json";
        JsonStreamingParser parser = new JsonStreamingParser(null, null, null, largeJsonFile);
        parser.parse();
    }
}
