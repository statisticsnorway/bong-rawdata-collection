package no.ssb.dc.collection.bong.ng;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.collection.api.config.GCSConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

// run test with Idea EnvVar:
// BONG_PASSWORD=password;BONG_SALT=salt;
public class BucketIntegrityTest {

    static final Logger LOG = LoggerFactory.getLogger(BucketIntegrityTest.class);
    static AtomicLong counter = new AtomicLong();

    void consumeGCSBucket(String bucket, String topic, String encryptionKey, String encryptionSalt, Consumer<RawdataConsumer> visitBeforeReceive, BiConsumer<RawdataMessage, String> visitMessage) {
        Objects.requireNonNull(encryptionKey);
        Objects.requireNonNull(encryptionSalt);

        GCSConfiguration gcsConfiguration = new GCSConfiguration(
                Map.of(
                        "target.rawdata.topic", topic,
                        "target.gcs.bucket-name", bucket,
                        "target.gcs.service-account.key-file", Paths.get(System.getProperty("user.home")).resolve("bin")
                                .resolve("ssb-team-dapla-rawdata-bong-dc28ff0c8faa.json").normalize().toAbsolutePath().toString(),
                        "target.local-temp-folder", "target/_tmp_avro_"
                )
        );
        EncryptionClient encryptionClient = new EncryptionClient();
        byte[] secretKey = encryptionClient.generateSecretKey(encryptionKey.toCharArray(), encryptionSalt.getBytes()).getEncoded();
        try (RawdataClient client = ProviderConfigurator.configure(gcsConfiguration.asMap(), "gcs", RawdataClientInitializer.class)) {
            try (RawdataConsumer consumer = client.consumer(gcsConfiguration.asDynamicConfiguration().evaluateToString("rawdata.topic"))) {
                visitBeforeReceive.accept(consumer);
                RawdataMessage message;
                while ((message = consumer.receive(5, TimeUnit.SECONDS)) != null) {
                    byte[] encryptedBytes = message.get("entry");
                    byte[] decryptedText = encryptionClient.decrypt(secretKey, encryptedBytes);
                    String data = new String(decryptedText, StandardCharsets.UTF_8);
                    visitMessage.accept(message, data);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Disabled
    @Test
    void thatRawdataStreamDoesntContainEmptyMessages() {
        consumeGCSBucket(
                "ssb-rawdata-prod-bong",
                "2018-10-ng-bong-202009071505",
                System.getenv("BONG_PASSWORD"),
                System.getenv("BONG_SALT"),
                consumer -> {
                },
                this::checkIfMessageContainsLessThanTwoCsvLines
        );
    }

    // used for locating a given message position
    void beforeReceiveSeekStreamPosition(RawdataConsumer consumer) {
        LOG.trace("Seek..");
        consumer.seek(ULID.parseULID("01EHGD1KFN0000000000000001").timestamp()); // replace with any ULID
    }

    void checkIfMessageContainsLessThanTwoCsvLines(RawdataMessage message, String data) {
        int lineCount = data.split("\n").length;
        LOG.trace("{}:\n{}", message.position(), data);
        if (counter.incrementAndGet() % 10000 == 0) {
            LOG.trace("Read: {}", counter.get());
        }
        if (lineCount <= 1) {
            LOG.error("{}: {}\n{}", message.position(), lineCount, data);
        }
    }

    @Disabled
    @Test
    void seekRawdataMessageOnStreamAndPrint() {
        consumeGCSBucket(
                "ssb-rawdata-prod-bong",
                "2018-10-ng-bong-202009071505",
                System.getenv("BONG_PASSWORD"),
                System.getenv("BONG_SALT"),
                this::beforeReceiveSeekStreamPosition,
                this::printMessage
        );
    }

    void printMessage(RawdataMessage message, String data) {
        int lineCount = data.split("\n").length;
//        LOG.trace("{}:\n{}", message.position(), data);
//        if (counter.incrementAndGet() % 10000 == 0) {
//            LOG.trace("Read: {}", counter.get());
//        }
        if (lineCount <= 1) {
            LOG.error("{}: {}\n{}", message.position(), lineCount, data);
        } else {
            LOG.info("{}: {}\n{}", message.position(), lineCount, data);
        }
    }

    @Disabled
    @Test
    void listCoopBongData() {
        consumeGCSBucket(
                "ssb-rawdata-prod-bong",
                "2018-10-coop-bong-202009080052",
                System.getenv("BONG_PASSWORD"),
                System.getenv("BONG_SALT"),
                consumer -> {
                },
                this::printMessage
        );
    }
}
