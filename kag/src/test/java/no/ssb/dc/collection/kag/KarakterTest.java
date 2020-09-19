package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Map;

public class KarakterTest {

    static SourceLmdbConfiguration sourceLmdbConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    static void beforeAll() {
        sourceLmdbConfiguration = new SourceLmdbConfiguration(Map.of(
                "source.lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString(),
                "source.rawdata.topic", "kag-source-test",
                "source.csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                "source.csv.files", "ssb_karakterfil.csv"
        ));

        targetConfiguration = new LocalFileSystemConfiguration(Map.of(
                "target.rawdata.topic", "kag-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    @Disabled
    @Test
    void prepare() {
        try (var worker = new KarakterWorker(sourceLmdbConfiguration, targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produce() {
        try (var worker = new KarakterWorker(sourceLmdbConfiguration, targetConfiguration)) {
            worker.produce();
        }
    }
}
