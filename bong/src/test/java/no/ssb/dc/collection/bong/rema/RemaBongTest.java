package no.ssb.dc.collection.bong.rema;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dapla.migration.rawdata.onprem.config.LocalFileSystemConfiguration;
import no.ssb.dapla.migration.rawdata.onprem.worker.ULIDGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RemaBongTest {

    static final Logger LOG = LoggerFactory.getLogger(RemaBongTest.class);

    static SourceRemaConfiguration sourceConfiguration;
    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    static void beforeAll() {
        sourceConfiguration = SourceRemaConfiguration.create(Map.of(
                "source.root.path", Paths.get(".").toAbsolutePath().normalize().resolve("data").toString(),
                "source.queue.capacity", "100",
                "source.year", "2018",
                "source.month", "10"
        ));

        targetConfiguration = LocalFileSystemConfiguration.create(Map.of(
                "target.rawdata.topic", "bong-rema-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    @Disabled
    @Test
    void validateFiles() {
        try (RemaBongWorker remaBongWorker = new RemaBongWorker(sourceConfiguration, targetConfiguration)) {
            boolean filesValidated = remaBongWorker.validate();
            LOG.trace("Files Validated: {}", filesValidated);
        }
    }

    @Disabled
    @Test
    void produceRawdata() {
        try (RemaBongWorker remaBongWorker = new RemaBongWorker(sourceConfiguration, targetConfiguration)) {
            remaBongWorker.produce();
        }
    }

    @Test
    void thatFilenameTimestampULIDIsValid() {
        RemaBongWorker.RawdataMessageBuffer writer1 = new RemaBongWorker.RawdataMessageBuffer(null, Paths.get("20180126_061507420-230101-237427"));
        RemaBongWorker.RawdataMessageBuffer writer2 = new RemaBongWorker.RawdataMessageBuffer(null, Paths.get("20180126_061507421-230101-237427"));
        ULID.Value ulid1 = ULIDGenerator.generate(writer1.toTimestamp());
        ULID.Value ulid2 = ULIDGenerator.generate(writer2.toTimestamp());
        assertNotEquals(ulid1, ulid2);
        LOG.trace("{}", ULIDGenerator.toUUID(ulid1));
        LOG.trace("{}", ULIDGenerator.toUUID(ulid2));
    }
}
