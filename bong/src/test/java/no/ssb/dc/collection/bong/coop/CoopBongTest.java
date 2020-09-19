package no.ssb.dc.collection.bong.coop;

import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;

class CoopBongTest {

    static final Logger LOG = LoggerFactory.getLogger(CoopBongTest.class);

    static SourcePostgresConfiguration sourcePostgresConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    static void beforeAll() {
        sourcePostgresConfiguration = new SourcePostgresConfiguration(
                Map.of(
                        "source.rawdata.topic", "bong-coop-source-test",
                        "source.csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                        "source.csv.files", "ssb_coop_1m_oct.csv"
                )
        );

        targetConfiguration = new LocalFileSystemConfiguration(Map.of(
                "target.rawdata.topic", "bong-coop-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    @Disabled
    @Test
    void buildDatabase() {
        try (var worker = new CoopPostgresBongWorker(sourcePostgresConfiguration, targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void readDatabaseAndCreateRawdata() {
        try (var worker = new CoopPostgresBongWorker(sourcePostgresConfiguration, targetConfiguration)) {
            worker.produce();
        }
    }
}
