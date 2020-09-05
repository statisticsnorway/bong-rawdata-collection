package no.ssb.dc.bong.rema;

import no.ssb.config.StoreBasedDynamicConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

class RemaBongTest {

    static final Logger LOG = LoggerFactory.getLogger(RemaBongTest.class);

    static StoreBasedDynamicConfiguration configuration;

    @BeforeAll
    static void beforeAll() {
        configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("rawdata.client.provider", "memory")
                .values("rawdata.topic", "bong-rema-test")
                .values("source.path", Paths.get(".").toAbsolutePath().normalize().resolve("src").resolve("test").resolve("resources").resolve("test-data").toString())
                .build();
    }

    @Test
    void scanFileSystem() {
        RemaBongRepository remaBongRepository = new RemaBongRepository(configuration);
        remaBongRepository.scanFileSystem();
    }
}
