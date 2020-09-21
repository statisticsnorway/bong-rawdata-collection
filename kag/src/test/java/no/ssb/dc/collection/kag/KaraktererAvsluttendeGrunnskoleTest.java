package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Map;

public class KaraktererAvsluttendeGrunnskoleTest {

    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    static void beforeAll() {
        targetConfiguration = new LocalFileSystemConfiguration(Map.of(
                "target.rawdata.topic", "kag-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    private static SourceLmdbConfiguration createSourceLmdbConfiguration(String sourceCsvFilename) {
        return new SourceLmdbConfiguration(Map.of(
                "source.lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString(),
                "source.rawdata.topic", "kag-source-test",
                "source.csv.filepath", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                "source.csv.files", sourceCsvFilename
        ));
    }

    @Disabled
    @Test
    void prepareKarakter() {
        try (var worker = new KarakterWorker(createSourceLmdbConfiguration("ssb_karakterfil.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceKarakter() {
        try (var worker = new KarakterWorker(createSourceLmdbConfiguration("ssb_karakterfil.csv"), targetConfiguration)) {
            worker.produce();
        }
    }
    @Disabled
    @Test
    void prepareResultat() {
        try (var worker = new ResultatWorker(createSourceLmdbConfiguration("ssb_resultatfil.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceResultat() {
        try (var worker = new ResultatWorker(createSourceLmdbConfiguration("ssb_resultatfil.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareFagkode() {
        try (var worker = new FagkodeWorker(createSourceLmdbConfiguration("ssb_fagkode.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceFagkode() {
        try (var worker = new FagkodeWorker(createSourceLmdbConfiguration("ssb_fagkode.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareStatistikk() {
        try (var worker = new StatistikkWorker(createSourceLmdbConfiguration("ssb_statistikk.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceStatistikk() {
        try (var worker = new StatistikkWorker(createSourceLmdbConfiguration("ssb_statistikk.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareNuskat() {
        try (var worker = new NuskatWorker(createSourceLmdbConfiguration("ssb_nuskat.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceNuskat() {
        try (var worker = new NuskatWorker(createSourceLmdbConfiguration("ssb_nuskat.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareSkolekatalog() {
        try (var worker = new SkolekatalogWorker(createSourceLmdbConfiguration("ssb_skolekatalog.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceSkolekatalog() {
        try (var worker = new SkolekatalogWorker(createSourceLmdbConfiguration("ssb_skolekatalog.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareOmkodingskatalog() {
        try (var worker = new OmkodingskatalogWorker(createSourceLmdbConfiguration("ssb_omkodingskatalog.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceOmkodingskatalog() {
        try (var worker = new OmkodingskatalogWorker(createSourceLmdbConfiguration("ssb_omkodingskatalog.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareGSI() {
        try (var worker = new GsiWorker(createSourceLmdbConfiguration("ssb_gsi.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceGSI() {
        try (var worker = new GsiWorker(createSourceLmdbConfiguration("ssb_gsi.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareNudb() {
        try (var worker = new NudbWorker(createSourceLmdbConfiguration("ssb_nudb.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceNudb() {
        try (var worker = new NudbWorker(createSourceLmdbConfiguration("ssb_nudb.csv"), targetConfiguration)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    void prepareNasjonaleProver() {
        try (var worker = new NasjonaleProverWorker(createSourceLmdbConfiguration("ssb_np.csv"), targetConfiguration)) {
            worker.prepare();
        }
    }

    @Disabled
    @Test
    void produceNasjonaleProver() {
        try (var worker = new NasjonaleProverWorker(createSourceLmdbConfiguration("ssb_np.csv"), targetConfiguration)) {
            worker.produce();
        }
    }
}
