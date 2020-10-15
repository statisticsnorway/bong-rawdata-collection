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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class SimpleCsvHandler<T extends RepositoryKey> implements CsvHandler<T> {

    static final Logger LOG = LoggerFactory.getLogger(SimpleCsvHandler.class);

    private final RawdataClient rawdataClient;
    private final TargetConfiguration targetConfiguration;
    private final CsvSpecification specification;
    private final Function<CsvParser.Record, T> produceRepositoryKey;
    private final SourceNoDbConfiguration sourceConfiguration;

    public SimpleCsvHandler(SourceNoDbConfiguration sourceConfiguration,
                            TargetConfiguration targetConfiguration,
                            CsvSpecification specification,
                            Function<CsvParser.Record, T> produceRepositoryKey) {
        this.sourceConfiguration = sourceConfiguration;
        rawdataClient = ProviderConfigurator.configure(targetConfiguration.asMap(), targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
        this.targetConfiguration = targetConfiguration;
        this.specification = specification;
        this.produceRepositoryKey = produceRepositoryKey;
    }

    @Override
    public void prepare(Function<CsvParser.Record, T> produceSortableKey) {
        // do nothing
    }

    @Override
    public void produce() {
        try (var producer = rawdataClient.producer(targetConfiguration.topic())) {
            AtomicLong handledRecords = new AtomicLong();
            var keyBuffer = ByteBuffer.allocateDirect(sourceConfiguration.queueKeyBufferSize());
            try (var bufferedRawdataProducer = new BufferedRawdataProducer(sourceConfiguration, targetConfiguration, specification, producer)) {
                var csvReader = new CsvReader(sourceConfiguration, specification);

                // TODO this must be consolidated with CsvRead.readAndBufferedWrite

                csvReader.parse(specification.fileDescriptor.delimiter, specification.fileDescriptor.charset, record -> {
                    if (BufferedRawdataProducer.readRecords.incrementAndGet() % 100000 == 0) {
                        LOG.info("Database - Read Records: {}", BufferedRawdataProducer.readRecords.get());
                    }

                    try {
                        T repositoryKey = produceRepositoryKey.apply(record);
                        repositoryKey.toByteBuffer(keyBuffer);

                        Map<T, String> recordSetMap = new LinkedHashMap<>();
                        recordSetMap.put(repositoryKey, record.asLine());
                        bufferedRawdataProducer.produceRawdataMessage(
                                BufferedRawdataProducer.RecordType.SINGLE,
                                record.filepath,
                                record.filename,
                                record.headers,
                                Character.toString(record.delimiter),
                                recordSetMap
                        );
                        handledRecords.incrementAndGet();

                        if (BufferedRawdataProducer.recordCount.incrementAndGet() % 10000 == 0) {
                            LOG.info("Database - Produced Record Count: {}", BufferedRawdataProducer.recordCount.get());
                        }

                    } finally {
                        keyBuffer.clear();
                    }
                });
            }

            LOG.debug("Read: {} -- Handled: {}", BufferedRawdataProducer.readRecords.get(), handledRecords.get());
            LOG.info("Database - Read Records Total: {}", BufferedRawdataProducer.readRecords.get());
            LOG.info("Database - Produced Records Total: {}", BufferedRawdataProducer.recordCount.get());

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
