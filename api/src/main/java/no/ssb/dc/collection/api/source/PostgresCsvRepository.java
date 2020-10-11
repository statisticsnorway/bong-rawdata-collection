package no.ssb.dc.collection.api.source;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.csv.CsvReader;
import no.ssb.dc.collection.api.jdbc.PostgresDataSource;
import no.ssb.dc.collection.api.jdbc.PostgresTransactionFactory;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresCsvRepository<T extends RepositoryKey> extends AbstractCsvRepository<T> {

    static final Logger LOG = LoggerFactory.getLogger(PostgresCsvRepository.class);
    private final HikariDataSource dataSource;
    private final PostgresTransactionFactory transactionFactory;
    private final RawdataClient rawdataClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SourcePostgresConfiguration sourceConfiguration;

    public PostgresCsvRepository(SourcePostgresConfiguration sourceConfiguration,
                                 TargetConfiguration targetConfiguration,
                                 CsvSpecification specification,
                                 Class<T> keyClass,
                                 BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        super(sourceConfiguration, targetConfiguration, specification, keyClass, isPrevKeyPartOfCurrentKey);
        this.sourceConfiguration = sourceConfiguration;
        this.dataSource = PostgresDataSource.openPostgresDataSource(this.sourceConfiguration);
        this.transactionFactory = new PostgresTransactionFactory(dataSource);
        this.rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
    }

    /**
     * Build Database with guaranteed sequence key ordering
     *
     * @param produceSortableKey The function must produce a sortable RepositoryKey
     */
    @Override
    public void prepare(Function<CsvParser.Record, T> produceSortableKey) {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        var csvReader = new CsvReader(sourceConfiguration, specification);
        try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory, specification)) {
            this.readCsvFileAndPrepareDatabase(csvReader, bufferedReadWrite, produceSortableKey);
        }
    }

    /**
     * Iterate source and consume position groups
     *
     * @param entrySetCallback a group of records by key
     */
    @Override
    public void consume(Consumer<Map<T, String>> entrySetCallback) {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory, specification)) {
            this.readDatabaseAndHandleGroup(bufferedReadWrite, entrySetCallback);
        }
    }

    /**
     * Produce Rawdata
     */
    @Override
    public void produce() {
        if (closed.get()) {
            throw new RuntimeException("Repository is closed!");
        }

        var producer = rawdataClient.producer(targetConfiguration.topic());

        try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(targetConfiguration, 1000, producer)) {
            try (BufferedReadWrite bufferedReadWrite = new PostgresBufferedReadWrite(sourceConfiguration, transactionFactory, specification)) {
                this.readDatabaseAndProduceRawdata(bufferedReadWrite, bufferedProducer);
            }

            Path targetLocalTempPath = Paths.get(targetConfiguration.localTempFolder() + "/" + targetConfiguration.topic());
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
