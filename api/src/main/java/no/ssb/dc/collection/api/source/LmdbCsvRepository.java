package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.csv.CsvReader;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LmdbCsvRepository<T extends RepositoryKey> extends AbstractCsvRepository<T> {

    static final Logger LOG = LoggerFactory.getLogger(LmdbCsvRepository.class);

    private final RawdataClient rawdataClient;
    private final SourceLmdbConfiguration sourceConfiguration;


    public LmdbCsvRepository(SourceLmdbConfiguration sourceConfiguration,
                             TargetConfiguration targetConfiguration,
                             CsvSpecification specification,
                             Class<T> keyClass,
                             BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        super(sourceConfiguration, targetConfiguration, specification, keyClass, isPrevKeyPartOfCurrentKey);
        this.sourceConfiguration = sourceConfiguration;
        rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
    }

    /**
     * Build Database with guaranteed sequence key ordering
     *
     * @param produceSortableKey The function must produce a sortable RepositoryKey
     */
    @Override
    public void prepare(Function<CsvParser.Record, T> produceSortableKey) {
        var csvReader = new CsvReader(sourceConfiguration, specification);
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, true)) {
            try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment, specification)) {
                this.readCsvFileAndPrepareDatabase(csvReader, bufferedReadWrite, produceSortableKey);
            }
        }
    }

    /**
     * Iterate source and consume position groups
     *
     * @param entrySetCallback a group of records by key
     */
    @Override
    public void consume(Consumer<Map<T, String>> entrySetCallback) {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment, specification)) {
                this.readDatabaseAndHandleGroup(bufferedReadWrite, entrySetCallback);
            }
        }
    }

    /**
     * Produce Rawdata
     */
    @Override
    public void  produce() {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            var producer = rawdataClient.producer(targetConfiguration.topic());

            try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(targetConfiguration, 1000, producer)) {
                try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment, specification)) {
                    this.readDatabaseAndProduceRawdata(bufferedReadWrite, bufferedProducer);
                }

                var targetLocalTempPath = Paths.get(targetConfiguration.localTempFolder() + "/" + targetConfiguration.topic());
                if (targetLocalTempPath.toFile().exists()) {
                    LOG.info("Avro-file Count: {}: {}\n\t{}", targetLocalTempPath.toString(), Files.list(targetLocalTempPath).count(),
                            Files.walk(targetLocalTempPath).filter(Files::isRegularFile).map(Path::toString).collect(Collectors.joining("\n\t")));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            rawdataClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
