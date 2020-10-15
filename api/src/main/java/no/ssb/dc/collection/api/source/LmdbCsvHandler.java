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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LmdbCsvHandler<T extends RepositoryKey> implements CsvHandler<T> {

    static final Logger LOG = LoggerFactory.getLogger(LmdbCsvHandler.class);

    private final RawdataClient rawdataClient;
    private final TargetConfiguration targetConfiguration;
    private final CsvSpecification specification;
    private final Class<T> keyClass;
    private final BiPredicate<T, T> isPrevKeyPartOfCurrentKey;
    private final SourceLmdbConfiguration sourceConfiguration;

    public LmdbCsvHandler(SourceLmdbConfiguration sourceConfiguration,
                          TargetConfiguration targetConfiguration,
                          CsvSpecification specification,
                          Class<T> keyClass,
                          BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        this.sourceConfiguration = sourceConfiguration;
        rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
        this.targetConfiguration = targetConfiguration;
        this.specification = specification;
        this.keyClass = keyClass;
        this.isPrevKeyPartOfCurrentKey = isPrevKeyPartOfCurrentKey;
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
                csvReader.readAndBufferedWrite(bufferedReadWrite, produceSortableKey);
            }
        }
    }

    /**
     * Produce Rawdata
     */
    @Override
    public void produce() {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            var producer = rawdataClient.producer(targetConfiguration.topic());

            try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(sourceConfiguration, targetConfiguration, specification, producer)) {
                try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment, specification)) {
                    bufferedProducer.readDatabaseAndProduceRawdata(bufferedReadWrite, keyClass, isPrevKeyPartOfCurrentKey);
                }

                var targetLocalTempPath = Paths.get(targetConfiguration.localTempFolder() + "/" + targetConfiguration.topic());
                if (Files.isReadable(targetLocalTempPath)) {
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
