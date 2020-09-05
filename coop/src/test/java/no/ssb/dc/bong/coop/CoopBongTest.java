package no.ssb.dc.bong.coop;

import no.ssb.config.StoreBasedDynamicConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

class CoopBongTest {

    static final Logger LOG = LoggerFactory.getLogger(CoopBongTest.class);

    static StoreBasedDynamicConfiguration configuration;

    @BeforeAll
    static void beforeAll() {
        configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("rawdata.client.provider", "memory")
                .values("lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString())
                .values("lmdb.sizeInMb", "500")
                .values("queue.poolSize", "25000") // flush buffer on threshold
                .values("queue.keyBufferSize", "511")
                .values("queue.valueBufferSize", "2048")
                .values("rawdata.topic", "bong-coop-test")
                .values("csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString())
                .values("csv.files", "ssb_ove_coop_1m_oct.csv")
                .values("csv.dateFormat", "yyyyMMddHHmmss")
                .build();
    }

    @Disabled
    @Test
    void buildDatabase() {
        CoopBongRepository repository = new CoopBongRepository(configuration);
        repository.buildDatabase();
    }

    @Disabled
    @Test
    void readDatabaseAndCreateRawdata() {
        CoopBongRepository repository = new CoopBongRepository(configuration);
        repository.produceRawdata();
    }
}
