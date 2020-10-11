package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.config.SourceNoDbConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.csv.CsvReader;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

public class SimpleCsvRepository<T extends RepositoryKey> extends AbstractCsvRepository<T> {

    private final RawdataClient rawdataClient;
    private final Function<CsvParser.Record, T> produceRepositoryKey;

    public SimpleCsvRepository(SourceNoDbConfiguration sourceConfiguration,
                               TargetConfiguration targetConfiguration,
                               CsvSpecification specification,
                               Class<T> keyClass,
                               BiPredicate<T, T> isPrevKeyPartOfCurrentKey,
                               Function<CsvParser.Record, T> produceRepositoryKey) {
        super(sourceConfiguration, targetConfiguration, specification, keyClass, isPrevKeyPartOfCurrentKey);
        rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
        this.produceRepositoryKey = produceRepositoryKey;
    }

    @Override
    public void prepare(Function<CsvParser.Record, T> produceSortableKey) {
        // do nothing
    }

    @Override
    public void consume(Consumer<Map<T, String>> entrySetCallback) {
        // do nothing
    }

    @Override
    public void produce() {
        try (var producer = rawdataClient.producer(targetConfiguration.topic())) {
            AtomicLong handledRecords = new AtomicLong();
            var keyBuffer = ByteBuffer.allocateDirect(511);
            try (var bufferedRawdataProducer = new BufferedRawdataProducer(targetConfiguration, 1000, producer)) {
                var csvReader = new CsvReader(sourceConfiguration, specification);
                csvReader.parse(specification.fileDescriptor.delimiter, specification.fileDescriptor.charset, record -> {
                    if (readRecords.incrementAndGet() % 100000 == 0) {
                        LOG.info("Database - Read Records: {}", readRecords.get());
                    }

                    try {
                        T repositoryKey = produceRepositoryKey.apply(record);
                        repositoryKey.toByteBuffer(keyBuffer);

                        Map<T, String> recordSetMap = new LinkedHashMap<>();
                        recordSetMap.put(repositoryKey, record.asLine());
                        this.produceRawdataMessage(bufferedRawdataProducer, record.filepath, record.filename, record.headers, Character.toString(record.delimiter), recordSetMap);
                        handledRecords.incrementAndGet();

                        if (recordCount.incrementAndGet() % 10000 == 0) {
                            LOG.info("Database - Produced Record Count: {}", recordCount.get());
                        }

                    } finally {
                        keyBuffer.clear();
                    }
                });
            }

            LOG.debug("Read: {} -- Handled: {}", readRecords.get(), handledRecords.get());
            LOG.info("Database - Read Records Total: {}", readRecords.get());
            LOG.info("Database - Produced Records Total: {}", recordCount.get());

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
