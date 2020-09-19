package no.ssb.dc.collection.api.source;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CSVReader;
import no.ssb.dc.collection.api.jdbc.PostgresDataSource;
import no.ssb.dc.collection.api.jdbc.PostgresTransactionFactory;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresCsvRepository<T extends RepositoryKey> extends AbstractCsvRepository<T> implements AutoCloseable {

    static final Logger LOG = LoggerFactory.getLogger(PostgresCsvRepository.class);
    private final HikariDataSource dataSource;
    private final PostgresTransactionFactory transactionFactory;
    private final RawdataClient rawdataClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public PostgresCsvRepository(SourcePostgresConfiguration sourceConfiguration,
                                 TargetConfiguration targetConfiguration,
                                 Class<T> keyClass,
                                 String delimeter,
                                 Charset csvCharset,
                                 String csvHeader,
                                 BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        super(sourceConfiguration, targetConfiguration, keyClass, delimeter, csvCharset, csvHeader, isPrevKeyPartOfCurrentKey);
        this.dataSource = PostgresDataSource.openPostgresDataSource(this.sourceConfiguration);
        this.transactionFactory = new PostgresTransactionFactory(dataSource);
        this.rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
    }

    /**
     * Build Database with guaranteed sequence key ordering
     *
     * @param produceSortableKey The function must produce a sortable RepositoryKey
     */
    public void prepare(Function<CSVReader.Record, T> produceSortableKey) {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        var csvReader = new CSVReader(sourceConfiguration, true);
        try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory)) {
            this.readCsvFileAndPrepareDatabase(csvReader, bufferedReadWrite, produceSortableKey);
        }
    }

    /**
     * Iterate source and consume position groups
     *
     * @param entrySetCallback a group of records by key
     */
    public void consume(Consumer<Map<T, String>> entrySetCallback) {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory)) {
            this.readDatabaseAndHandleGroup(bufferedReadWrite, entrySetCallback);
        }
    }

    /**
     * Produce Rawdata
     */
    public void produce() {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        var producer = rawdataClient.producer(targetConfiguration.evaluateToString("rawdata.topic"));

        try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(targetConfiguration, 1000, producer)) {
            try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory)) {
                this.readDatabaseAndProduceRawdata(bufferedReadWrite, producer, bufferedProducer);
            }

            Path targetLocalTempPath = Paths.get(targetConfiguration.evaluateToString("local-temp-folder") + "/" + targetConfiguration.evaluateToString("rawdata.topic"));
            if (targetLocalTempPath.toFile().exists()) {
                LOG.info("Avro-file Count: {}: {}\n\t{}", targetLocalTempPath.toString(), Files.list(targetLocalTempPath).count(),
                        Files.walk(targetLocalTempPath).filter(Files::isRegularFile).map(Path::toString).collect(Collectors.joining("\n\t")));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                rawdataClient.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            transactionFactory.close();
            dataSource.close();
        }
    }
}
