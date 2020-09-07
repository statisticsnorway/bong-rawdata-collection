package no.ssb.dc.bong.ng.repository;

import no.ssb.dc.bong.commons.config.LocalFileSystemConfiguration;
import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.rawdata.BufferedRawdataProducer;
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

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class NGBongTest {

    static final Logger LOG = LoggerFactory.getLogger(NGBongTest.class);

    static SourceLmdbConfiguration sourceLmdbConfiguration;
    static SourcePostgresConfiguration sourcePostgresConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    static void beforeAll() {
        sourceLmdbConfiguration = new SourceLmdbConfiguration(Map.of(
                "source.lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString(),
                "source.rawdata.topic", "bong-ng-source-test",
                "source.csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                "source.csv.files", "ssb_ove_1m_oct.csv",
                "source.lmdb.sizeInMb", "116340"
        ));

        sourcePostgresConfiguration = new SourcePostgresConfiguration(
                Map.of(
                        "source.rawdata.topic", "bong-ng-source-test",
                        "source.csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                        "source.csv.files", "ssb_ove_1m_oct.csv"
                )
        );

        targetConfiguration = new LocalFileSystemConfiguration(Map.of(
                "target.rawdata.topic", "bong-ng-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    @Disabled
    @Test
    void buildDatabase() {
        NGLmdbBongRepository repository = new NGLmdbBongRepository(sourceLmdbConfiguration, targetConfiguration);
        repository.prepare();
    }

    @Disabled
    @Test
    void produceRawdata() {
        NGLmdbBongRepository repository = new NGLmdbBongRepository(sourceLmdbConfiguration, targetConfiguration);
        repository.produce();
    }

    @Disabled
    @Test
    void buildPostgresDatabase() {
        NGPostgresBongRepository repository = new NGPostgresBongRepository(sourcePostgresConfiguration, targetConfiguration);
        repository.prepare();
    }

    @Disabled
    @Test
    void readDatabase() {
        AtomicLong counter = new AtomicLong();
        NGPostgresBongRepository repository = new NGPostgresBongRepository(sourcePostgresConfiguration, targetConfiguration);
        repository.consume((Map<NGBongKey, String> bong) -> {
            for (Map.Entry<NGBongKey, String> entry : bong.entrySet()) {
                LOG.trace("{}: {}", "", entry.getValue());
                if (counter.incrementAndGet() % 10000 == 0) {
                    LOG.trace("{}", counter.get());
                }
            }
        });
        LOG.trace("{}", counter.get());
    }

    @Disabled
    @Test
    void producePostgresRawdata() {
        NGPostgresBongRepository repository = new NGPostgresBongRepository(sourcePostgresConfiguration, targetConfiguration);
        repository.produce();
    }

    @Disabled
    @Test
    void consumeFilesystemAvroData() {
        try (RawdataClient client = ProviderConfigurator.configure(targetConfiguration.asDynamicConfiguration().asMap(), "filesystem", RawdataClientInitializer.class)) {
            try (RawdataConsumer consumer = client.consumer(targetConfiguration.asDynamicConfiguration().evaluateToString("rawdata.topic"))) {
                RawdataMessage message;
                while ((message = consumer.receive(15, TimeUnit.SECONDS)) != null) {
                    LOG.trace("{}: {}", message.position(), new String(message.get("entry")));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Disabled
    @Test
    void consumeAndBufferAndProduce() {
        try (RawdataClient client = ProviderConfigurator.configure(targetConfiguration.asMap(), "filesystem", RawdataClientInitializer.class)) {
            String targetTopic = targetConfiguration.asDynamicConfiguration().evaluateToString("rawdata.topic");
            try (RawdataConsumer consumer = client.consumer(targetTopic)) {
                try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(1000, client.producer(targetTopic))) {
                    RawdataMessage message;
                    while ((message = consumer.receive(15, TimeUnit.SECONDS)) != null) {
                        bufferedProducer.produce(message);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
