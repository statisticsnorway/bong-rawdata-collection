package no.ssb.dc.bong.coop;

import no.ssb.dc.bong.commons.config.LocalFileSystemConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
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
                        "source.csv.files", "ssb_ove_coop_1m_oct.csv"
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
        CoopPostgresBongRepository repository = new CoopPostgresBongRepository(sourcePostgresConfiguration, targetConfiguration);
        repository.prepare();
    }

    @Disabled
    @Test
    void readDatabaseAndCreateRawdata() {
        CoopPostgresBongRepository repository = new CoopPostgresBongRepository(sourcePostgresConfiguration, targetConfiguration);
        repository.produce();
    }
}