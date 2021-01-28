package no.ssb.dc.collection.kostra;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.collection.api.config.GCSConfiguration;
import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.worker.JsonParser;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KostraTest {

    static final Logger LOG = LoggerFactory.getLogger(KostraTest.class);

    static SourceKostraConfiguration sourceConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;
    static GCSConfiguration gcsTargetConfiguration;
    private static DynamicConfiguration secretConfig;

    @BeforeAll
    public static void beforeAll() {
        secretConfig = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-secret.properties")
                .build();

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

        gcsTargetConfiguration = GCSConfiguration.create(Map.of(
                "target.rawdata.topic", "2021-01-28-kostra-regnskap-2019",
                "target.gcs.bucket-name", "ssb-rawdata-prod-kostra",
                "target.gcs.service-account.key-file", secretConfig.evaluateToString("gcs.service-account.key-file"),
                "target.local-temp-folder", "target/_tmp_avro_"
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

    static byte[] tryDecryptContent(EncryptionClient encryptionClient, byte[] secretKey, byte[] content) {
        if (secretKey != null && content != null) {
            return encryptionClient.decrypt(secretKey, content);
        }
        return content;
    }

    @Disabled
    @Test
    public void consumeRawdataIntegration() throws Exception {
        EncryptionClient encryptionClient = new EncryptionClient();
        byte[] secretKey = encryptionClient.generateSecretKey(secretConfig.evaluateToString("encryption.key").toCharArray(), secretConfig.evaluateToString("encryption.salt").getBytes()).getEncoded();

        AtomicLong counter = new AtomicLong();

        try (RawdataClient client = ProviderConfigurator.configure(gcsTargetConfiguration.asMap(), gcsTargetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class)) {
            try (RawdataConsumer consumer = client.consumer(gcsTargetConfiguration.topic())) {
                RawdataMessage message;
                while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                    if (counter.incrementAndGet() > 10) break; // stopAt break loop

                    String manifestJsonAsString = new String(message.get("manifest.json"));
                    String dataJsonAsString = new String(tryDecryptContent(encryptionClient, secretKey, message.get("entry")));

                    JsonNode manifestJsonNode = JsonParser.createJsonParser().fromJson(manifestJsonAsString, JsonNode.class);
                    JsonNode dataNode = JsonParser.createJsonParser().fromJson(dataJsonAsString, JsonNode.class);

                    LOG.trace("pos: {}:\nmanifest: {}\ndata: {}", message.position(),
                            JsonParser.createJsonParser().toPrettyJSON(manifestJsonNode),
                            JsonParser.createJsonParser().toPrettyJSON(dataNode)
                    );
                }
            }
        }
    }

}
