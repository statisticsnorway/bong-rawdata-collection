package no.ssb.dc.collection.api.worker;

import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.SourceNoDbConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

import static no.ssb.dc.collection.api.worker.CsvSpecification.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvDynamicTest {

    private static final Logger LOG = LoggerFactory.getLogger(CsvDynamicTest.class);

    static final CsvSpecification specification = new CsvSpecification.Builder()
            .backend(BackendProvider.NO_CACHE)
            .metadata(metadata()
                    .source("source")
                    .dataset("dataset")
                    .tag("tag")
                    .description("bla bla")
            )
            .fileDescriptor(fileDescriptor()
                    .delimiter(';')
                    .charset(StandardCharsets.US_ASCII)
                    .contentType("text/csv")
                    .files("generic.csv")
            )
            .positionKey(columnKey()
                    .name("a")
            )
            .positionKey(function()
                    .generator(KeyGenerator.ULID)
            )
            .groupByKey(column()
                    .name("a")
                    .type(String.class)
            )
            .groupByKey(column()
                    .name("b")
                    .type(String.class)
            )
            .groupByKey(column()
                    .name("c")
                    .type(Date.class)
                    .format("MM/dd/yyyy HH:mm:ss")
            )
            .build();

    static LocalFileSystemConfiguration targetConfiguration;

    @BeforeAll
    public static void beforeAll() {
        targetConfiguration = LocalFileSystemConfiguration.create(Map.of(
                "target.rawdata.topic", "dummy-target-test",
                "target.local-temp-folder", "target/_tmp_avro_",
                "target.filesystem.storage-folder", "target/rawdata-store"
        ));
    }

    private static SourceNoDbConfiguration createSourceNoDbConfiguration(String sourceCsvFilename) {
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get("target/test-classes/no/ssb/dc/collection/api/worker"));
        assertTrue(Files.isDirectory(targetPath));
        return SourceNoDbConfiguration.create(Map.of(
                "source.rawdata.topic", "dummy-source-test",
                "source.csv.filepath", targetPath.toString(),
                "source.csv.files", sourceCsvFilename
        ));
    }

    private static SourceLmdbConfiguration createSourceLmdbConfiguration(String sourceCsvFilename) {
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get("target/test-classes/no/ssb/dc/collection/api/worker"));
        assertTrue(Files.isDirectory(targetPath));
        return SourceLmdbConfiguration.create(Map.of(
                "source.lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString(),
                "source.rawdata.topic", "dummy-source-test",
                "source.csv.filepath", targetPath.toString(),
                "source.csv.files", sourceCsvFilename
        ));
    }

    private static SourcePostgresConfiguration createSourcePostgresConfiguration(String sourceCsvFilename) {
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get("target/test-classes/no/ssb/dc/collection/api/worker"));
        assertTrue(Files.isDirectory(targetPath));
        return SourcePostgresConfiguration.create(
                Map.of(
                        "source.rawdata.topic", "dummy-source-test",
                        "source.csv.filepath", targetPath.toString(),
                        "source.csv.files", sourceCsvFilename
                )
        );
    }

    @Test
    public void spec() {
        assertEquals(StandardCharsets.US_ASCII, specification.fileDescriptor.charset);
        assertEquals("a", specification.positionKeys.get("a").internal());
        assertEquals(String.class, specification.groupByColumns.get("a").type);
        assertEquals(String.class, specification.groupByColumns.get("b").type);
    }

    @Test
    public void deserializeSpec() throws IOException {
        SpecificationDeserializer deserializer = new SpecificationDeserializer();
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get("target/test-classes/no/ssb/dc/collection/api/worker"));
        assertTrue(Files.isDirectory(targetPath));
        CsvSpecification specification = deserializer.parse(Files.readString(targetPath.resolve("generic-spec.yaml"), StandardCharsets.UTF_8));
        deserializer.validate(specification);
        LOG.trace("spec: {}", specification);
    }

    @Test
    public void metadataContent() {
        MetadataContent metadataContent = new MetadataContent.Builder()
                .topic("topic")
                .position("1")
                .contentKey("entry")
                .contentType("text/csv")
                .contentLength(10)
                .markCreatedDate()
                .sourceFile("my.csv")
                .description("bla bal")
                .build();
        String md = JsonParser.createJsonParser().toPrettyJSON(metadataContent.getElementNode());
        LOG.trace("{}", md);
    }

    @Disabled
    @Test
    void simpleWorker() {
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourceNoDbConfiguration("generic.csv"), targetConfiguration, specification)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    public void lmdbWorker() {
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourceLmdbConfiguration("generic.csv"), targetConfiguration, specification)) {
            worker.prepare();
            worker.produce();
        }
    }

    @Disabled
    @Test
    public void postgresWorker() {
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourcePostgresConfiguration("generic.csv"), targetConfiguration, specification)) {
            worker.prepare();
            worker.produce();
        }
    }

}
