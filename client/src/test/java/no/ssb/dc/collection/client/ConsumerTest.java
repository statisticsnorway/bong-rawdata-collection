package no.ssb.dc.collection.client;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Pre-requisites:
 * <p>
 * - Create SA account for bucket and store it locally in a secure place
 * - The Bucket secrets is found on the Dapla server under `/dapla/secret/trosamf-encryption-key.env`
 * <p>
 * 1. Copy `application-secret.properties.sample` to `application-secret.properties`
 * 2. Configure `application-secret.properties.sample`
 * 3. Run test method `consume()`
 */

public class ConsumerTest {

    static final Logger LOG = LoggerFactory.getLogger(ConsumerTest.class);

    /*
        Enable the topic you want to consume.
     */

    //static final String RAWDATA_TOPIC = "TRO19IKKE_HAE-202101181556";
    //static final String RAWDATA_TOPIC = "TRO18IKKE19_HAE-202101151454";
    static final String RAWDATA_TOPIC = "TRO18IKKE19_HAE-202101201110";

    static final DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .propertiesResource("application-gcs.properties")
            .propertiesResource("application-secret.properties")
            .values("gcs.bucket-name", "ssb-rawdata-prod-trosamf")
            .values("rawdata.topic", RAWDATA_TOPIC)
            .build();

    private static EncryptionClient encryptionClient;
    private static byte[] secretKey;

    @BeforeAll
    static void beforeAll() {
        encryptionClient = new EncryptionClient();

        if (configuration.evaluateToString("bucket.encryption.key") != null) {
            secretKey = encryptionClient.generateSecretKey(
                    configuration.evaluateToString("bucket.encryption.key").toCharArray(),
                    configuration.evaluateToString("bucket.encryption.salt").getBytes()
            ).getEncoded();
        }
    }

    @Disabled
    @Test
    void consume() throws Exception {
        try (RawdataClient client = ProviderConfigurator.configure(
                configuration.asMap(),
                configuration.evaluateToString("rawdata.client.provider"),
                RawdataClientInitializer.class
        )) {
            try (RawdataConsumer consumer = client.consumer(configuration.evaluateToString("rawdata.topic"))) {
                RawdataMessage message;
                // read each message from stream
                while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {

                    // print message
                    StringBuilder contentBuilder = new StringBuilder();
                    contentBuilder.append("\nposition: ").append(message.position());
                    for (String key : message.keys()) {
                        contentBuilder.append("\n\t").append(key).append(" => ").append(getMessage(key, message));
                    }
                    LOG.debug("{}", contentBuilder.toString());
                }
            }
        }
    }

    private String getMessage(String key, RawdataMessage message) {
        final byte[] decryptContent = tryDecryptContent(message.get(key));
        return new String(decryptContent, StandardCharsets.UTF_8);
    }

    static byte[] tryDecryptContent(byte[] content) {
        if (secretKey != null && content != null) {
            return encryptionClient.decrypt(secretKey, content);
        }
        return content;
    }

}
