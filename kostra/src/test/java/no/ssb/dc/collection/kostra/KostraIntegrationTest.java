package no.ssb.dc.collection.kostra;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.migration.rawdata.onprem.config.GCSConfiguration;
import no.ssb.dc.migration.rawdata.onprem.worker.JsonParser;
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

public class KostraIntegrationTest {

    static final Logger LOG = LoggerFactory.getLogger(KostraIntegrationTest.class);

    static DynamicConfiguration secretConfig;
    static SourceKostraConfiguration sourceConfiguration;
    static GCSConfiguration gcsTargetConfiguration;

    @BeforeAll
    public static void beforeAll() {
        /*
         * Copy sample to secret props and configure your local env
         */
        secretConfig = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-secret.properties")
                .build();

        /*
         * Local source configuration
         */
        sourceConfiguration = SourceKostraConfiguration.create(Map.of(
                "source.path", Paths.get(".").normalize().toAbsolutePath().resolve(Paths.get("src/test/resources/data")).toString(),
                "source.file", "kostradata.json",
                "source.specification.file", "kostradata-spec.yaml",
                "source.queue.capacity", "100"
        ));

        /*
         * RawdataClient Google Cloud Storage (GCS) configuration
         */
        gcsTargetConfiguration = GCSConfiguration.create(Map.of(
                "target.rawdata.topic", "2021-01-28-kostra-regnskap-2019",
                "target.gcs.bucket-name", "ssb-rawdata-prod-kostra",
                "target.gcs.service-account.key-file", secretConfig.evaluateToString("gcs.service-account.key-file") != null ? secretConfig.evaluateToString("gcs.service-account.key-file") : "",
                "target.local-temp-folder", "target/_tmp_avro_"
        ));
    }

    static byte[] tryDecryptContent(EncryptionClient encryptionClient, byte[] secretKey, byte[] content) {
        if (secretKey != null && content != null) {
            return encryptionClient.decrypt(secretKey, content);
        }
        return content;
    }

    @Disabled
    @Test
    public void consumeRawdata() throws Exception {
        EncryptionClient encryptionClient = new EncryptionClient();
        byte[] secretKey = encryptionClient.generateSecretKey(secretConfig.evaluateToString("encryption.key").toCharArray(), secretConfig.evaluateToString("encryption.salt").getBytes()).getEncoded();

        AtomicLong counter = new AtomicLong();

        try (RawdataClient client = ProviderConfigurator.configure(gcsTargetConfiguration.asMap(), gcsTargetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class)) {
            try (RawdataConsumer consumer = client.consumer(gcsTargetConfiguration.topic())) {
                RawdataMessage message;
                while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                    if (counter.incrementAndGet() > 10) break; // stopAt break loop

                    String manifestJsonAsString = new String(tryDecryptContent(encryptionClient, secretKey, message.get("manifest.json")));
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
