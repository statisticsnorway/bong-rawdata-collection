package no.ssb.dc.collection.kostra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.worker.JsonParser;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KostraTest {

    static final Logger LOG = LoggerFactory.getLogger(KostraTest.class);

    static SourceKostraConfiguration sourceConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    public static void beforeAll() {
        sourceConfiguration = SourceKostraConfiguration.create(Map.of(
                "source.path", Paths.get(".").normalize().toAbsolutePath().resolve(Paths.get("src/test/resources/data")).toString(),
                "source.file", "kostradata.json",
                "source.specification.file", "kostradata-spec.yaml",
                "source.queue.capacity", "100"
        ));

        targetConfiguration = LocalFileSystemConfiguration.create(Map.of(
                "target.rawdata.topic", "kostra-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    @Disabled
    @Test
    public void produceRawdata() {
        try (KostraWorker kostraWorker = new KostraWorker(sourceConfiguration, targetConfiguration)) {
            kostraWorker.produce();
        }
    }

    @Disabled
    @Test
    public void consumeRawdata() throws Exception {
        try (RawdataClient client = ProviderConfigurator.configure(targetConfiguration.asMap(), targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class)) {
            try (RawdataConsumer consumer = client.consumer(targetConfiguration.topic())) {
                RawdataMessage message;
                while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                    JsonNode manifestJsonNode = JsonParser.createJsonParser().fromJson(new String(message.get("manifest.json")), JsonNode.class);
                    JsonNode dataNode = JsonParser.createJsonParser().fromJson(new String(message.get("entry")), JsonNode.class);

                    LOG.trace("pos: {}:\nmanifest: {}\ndata: {}", message.position(),
                            JsonParser.createJsonParser().toPrettyJSON(manifestJsonNode),
                            JsonParser.createJsonParser().toPrettyJSON(dataNode)
                    );
                }
            }
        }
    }


    @Disabled
    @Test
    public void parseKostraJson() throws IOException {
        Path testData = Paths.get(".").normalize().toAbsolutePath().resolve(Paths.get("src/test/resources/data/kostradata.json"));
        try (InputStream is = new FileInputStream(testData.toFile())) {
            JsonNode root = JsonParser.createJsonParser().fromJson(is, JsonNode.class);
            ArrayNode structure = (ArrayNode) root.get("structure");
            // TODO convert structure[].name and .type to schema mapping
            ArrayNode data = (ArrayNode) root.get("data");
            // TODO iterate each element and write structure + one element per message
            for (int i = 0; i < data.size(); i++) {
                ArrayNode node = (ArrayNode) data.get(i);
                ObjectNode doc = JsonParser.createJsonParser().createObjectNode();
                doc.set("structure", structure);
                doc.set("data", node);
                LOG.trace("{}", JsonParser.createJsonParser().toPrettyJSON(doc));
            }
        }
    }

}
