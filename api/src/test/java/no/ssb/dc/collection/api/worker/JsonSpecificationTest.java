package no.ssb.dc.collection.api.worker;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static no.ssb.dc.collection.api.worker.JsonSpecification.*;

public class JsonSpecificationTest {

    static final Logger LOG = LoggerFactory.getLogger(JsonSpecificationTest.class);

    static final JsonSpecification specification = new JsonSpecification.Builder()
            .metadata(metadata()
                    .source("source")
                    .dataset("ds")
                    .tag("tag")
                    .description("desc")
            )
            .fileDescriptor(fileDescriptor()
                    .charset(StandardCharsets.UTF_8)
                    .contentType("application/json")
                    .files("example.json")
            )
            .identifiers(sequence()
                    .name("array")  // array or split, enum?
                    //.splt("")
                    //.expected("")
                    .position()
            )
            .identifiers(function()
                    .name("index")
                    .generator(KeyGenerator.SEQUENCE)
                    .position()
            )
            .build();

    @Test
    void spec() {
        String json = JsonParser.createJsonParser().toPrettyJSON(specification);
        LOG.trace("{}", json);
    }
}
