package no.ssb.dc.bong.commons.source;

import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.csv.CSVReader;
import no.ssb.dc.bong.commons.target.BufferedRawdataProducer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LmdbCsvRepository<T extends RepositoryKey> extends AbstractCsvRepository<T> implements AutoCloseable {

    static final Logger LOG = LoggerFactory.getLogger(LmdbCsvRepository.class);

    private final RawdataClient rawdataClient;

    public LmdbCsvRepository(SourceLmdbConfiguration sourceConfiguration,
                             TargetConfiguration targetConfiguration,
                             Class<T> keyClass,
                             String delimeter,
                             Charset csvCharset,
                             String csvHeader,
                             BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        super(sourceConfiguration, targetConfiguration, keyClass, delimeter, csvCharset, csvHeader, isPrevKeyPartOfCurrentKey);
        rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
    }

    /**
     * Build Database with guaranteed sequence key ordering
     *
     * @param produceSortableKey The function must produce a sortable RepositoryKey
     */
    public void prepare(Function<CSVReader.Record, T> produceSortableKey) {
        var csvReader = new CSVReader(sourceConfiguration, true);
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, true)) {
            try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment)) {
                this.readCsvFileAndPrepareDatabase(csvReader, bufferedReadWrite, produceSortableKey);
            }
        }
    }

    /**
     * Iterate source and consume position groups
     *
     * @param entrySetCallback a group of records by key
     */
    public void consume(Consumer<Map<T, String>> entrySetCallback) {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment)) {
                this.readDatabaseAndHandleGroup(bufferedReadWrite, entrySetCallback);
            }
        }
    }

    /**
     * Produce Rawdata
     */
    public void produce() {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            var producer = rawdataClient.producer(targetConfiguration.evaluateToString("rawdata.topic"));

            try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(targetConfiguration, 1000, producer)) {
                try (BufferedReadWrite bufferedReadWrite = new LmdbBufferedReadWrite(sourceConfiguration, lmdbEnvironment)) {
                    this.readDatabaseAndProduceRawdata(bufferedReadWrite, producer, bufferedProducer);
                }

                var targetLocalTempPath = Paths.get(targetConfiguration.evaluateToString("local-temp-folder") + "/" + targetConfiguration.evaluateToString("rawdata.topic"));
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
