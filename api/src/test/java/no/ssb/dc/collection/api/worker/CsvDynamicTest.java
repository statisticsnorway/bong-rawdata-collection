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
                    .files("example-1.csv")
            )
            .columnKeys(column()
                    .name("a")
                    .type(String.class)
                    .position()
                    .groupBy()
            )
            .columnKeys(column()
                    .name("b")
                    .type(String.class)
                    .groupBy()
            )
            .columnKeys(column()
                    .name("c")
                    .type(Date.class)
                    .format("MM/dd/yyyy HH:mm:ss")
                    .groupBy()
            )
            .columnKeys(function()
                    .name("seq")
                    .generator(KeyGenerator.SEQUENCE)
                    .position()
            )
            .build();
    public static final String SPEC_AND_DATA_RESOURCES = "src/test/resources/no/ssb/dc/collection/api/worker";

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
        Path currentPath = Paths.get(".").toAbsolutePath().normalize();
        Path targetPath = currentPath.resolve(Paths.get(SPEC_AND_DATA_RESOURCES));
        assertTrue(Files.isDirectory(targetPath));
        return SourceNoDbConfiguration.create(Map.of(
                "source.rawdata.topic", "dummy-source-test",
                "source.csv.filepath", targetPath.resolve(Paths.get("files/example/dummy/2020")).toString(),
                "source.csv.files", sourceCsvFilename
        ));
    }

    private static SourceLmdbConfiguration createSourceLmdbConfiguration(String sourceCsvFilename) {
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get(SPEC_AND_DATA_RESOURCES));
        assertTrue(Files.isDirectory(targetPath));
        return SourceLmdbConfiguration.create(Map.of(
                "source.lmdb.path", Paths.get(".").toAbsolutePath().normalize().resolve("target").resolve("lmdb").toString(),
                "source.rawdata.topic", "dummy-source-test",
                "source.csv.filepath", targetPath.resolve(Paths.get("files/example/dummy/2020")).toString(),
                "source.csv.files", sourceCsvFilename
        ));
    }

    private static SourcePostgresConfiguration createSourcePostgresConfiguration(String sourceCsvFilename) {
        Path targetPath = Paths.get(".").toAbsolutePath().normalize().resolve(Paths.get(SPEC_AND_DATA_RESOURCES));
        assertTrue(Files.isDirectory(targetPath));
        return SourcePostgresConfiguration.create(
                Map.of(
                        "source.rawdata.topic", "dummy-source-test",
                        "source.csv.filepath", targetPath.resolve(Paths.get("files/example/dummy/2020")).toString(),
                        "source.csv.files", sourceCsvFilename
                )
        );
    }

    @Test
    public void spec() {
        assertEquals(StandardCharsets.US_ASCII, specification.fileDescriptor.charset);
        assertEquals("a", specification.columns.columnKey("a").name);
        assertEquals(String.class, specification.columns.columnKey("a").type);
        assertEquals(String.class, specification.columns.columnKey("b").type);
    }

    @Test
    public void deserializeSpec() throws IOException {
        SpecificationDeserializer deserializer = new SpecificationDeserializer();
        Path currentPath = Paths.get(".").toAbsolutePath().normalize();
        LOG.trace("CurrentPath: {}", currentPath);
        Path targetPath = currentPath.resolve(Paths.get(SPEC_AND_DATA_RESOURCES));
        assertTrue(Files.isDirectory(targetPath));
        CsvSpecification specification = deserializer.parse(Files.readString(targetPath.resolve(Paths.get("spec/example/dummy/2020/example-spec.yaml")), StandardCharsets.UTF_8));
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
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourceNoDbConfiguration("example-1.csv"), targetConfiguration, specification)) {
            worker.produce();
        }
    }

    @Disabled
    @Test
    public void lmdbWorker() {
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourceLmdbConfiguration("example-1.csv"), targetConfiguration, specification)) {
            worker.prepare();
            worker.produce();
        }
    }

    @Disabled
    @Test
    public void postgresWorker() {
        try (CsvDynamicWorker worker = new CsvDynamicWorker(createSourcePostgresConfiguration("example-1.csv"), targetConfiguration, specification)) {
            worker.prepare();
            worker.produce();
        }
    }

}
