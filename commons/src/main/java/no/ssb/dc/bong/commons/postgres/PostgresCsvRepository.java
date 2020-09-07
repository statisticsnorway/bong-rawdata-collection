package no.ssb.dc.bong.commons.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.csv.CSVReader;
import no.ssb.dc.bong.commons.rawdata.BaseCsvRepository;
import no.ssb.dc.bong.commons.rawdata.BufferedRawdataProducer;
import no.ssb.dc.bong.commons.rawdata.RepositoryKey;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract public class PostgresCsvRepository<T extends RepositoryKey> extends BaseCsvRepository<T> {

    static final Logger LOG = LoggerFactory.getLogger(PostgresCsvRepository.class);

        static final AtomicLong csvReadLines = new AtomicLong(0);
    static final AtomicLong readRecords = new AtomicLong(0);
    static final AtomicLong groupCount = new AtomicLong(0);

    protected final DynamicConfiguration sourceConfiguration;
    protected final DynamicConfiguration targetConfiguration;
    private final Class<T> keyClass;
    private final String delimeter;
    protected final Charset csvCharset;

    public PostgresCsvRepository(SourcePostgresConfiguration sourceConfiguration, TargetConfiguration targetConfiguration, Class<T> keyClass, String delimeter, Charset csvCharset) {
        this.sourceConfiguration = sourceConfiguration.asDynamicConfiguration();
        this.targetConfiguration = targetConfiguration.asDynamicConfiguration();
        this.keyClass = keyClass;
        this.delimeter = delimeter;
        this.csvCharset = csvCharset;
    }

    /**
     * Build Database with guaranteed sequence key ordering
     *
     * @param produceSortableKey The function must produce a sortable RepositoryKey
     */
    protected void prepare(Function<CSVReader.Record, T> produceSortableKey) {
        CSVReader csvReader = new CSVReader(sourceConfiguration, true);
        try (HikariDataSource dataSource = PostgresDataSource.openPostgresDataSource(sourceConfiguration)) {
            PostgresTransactionFactory transactionFactory = new PostgresTransactionFactory(dataSource);
            try (PostgresBufferedWriter bufferedWriter = new PostgresBufferedWriter(sourceConfiguration, transactionFactory)) {
                csvReader.parse(delimeter, csvCharset, record -> {
                    RepositoryKey key = produceSortableKey.apply(record);

                    bufferedWriter.writeRecord(key, record.line);

                    if (csvReadLines.incrementAndGet() % 100000 == 0) {
                        LOG.info("Source - Read lines: {}", csvReadLines.get());
                    }
                });
            }
            LOG.info("Source - Read Lines Total: {}", csvReadLines.get());
        }
    }

    /**
     * Iterate source and consume position groups
     *
     * @param entrySetCallback          a group of records by key
     * @param isPrevKeyPartOfCurrentKey test if previous key is part of current key (this determines a group)
     */
    protected void consume(Consumer<Map<T, String>> entrySetCallback, BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        try (HikariDataSource dataSource = PostgresDataSource.openPostgresDataSource(sourceConfiguration)) {
            PostgresTransactionFactory transactionFactory = new PostgresTransactionFactory(dataSource);
            try (PostgresBufferedWriter bufferedWriter = new PostgresBufferedWriter(sourceConfiguration, transactionFactory)) {

                AtomicReference<Map.Entry<T, String>> prevEntry = new AtomicReference<>();
                Map<T, String> groupAndContentMap = new LinkedHashMap<>();

                bufferedWriter.readRecord(keyClass, (entry, hasNext) -> {
                    if (readRecords.incrementAndGet() % 100000 == 0) {
                        LOG.info("Postgres - Read Records: {}", readRecords.get());
                    }
                    T currentKey = entry.getKey();

                    // mark first position
                    if (prevEntry.get() == null) {
                        prevEntry.set(entry);
                        return;
                    }

                    //if (prevEntry.get().getKey().isPartOfBong(currentKey)) {
                    if (isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentKey)) {
                        // add previous keyValue if absent
                        if (!groupAndContentMap.containsKey(prevEntry.get().getKey())) {
                            groupAndContentMap.put(prevEntry.get().getKey(), prevEntry.get().getValue());
                        }

                        // add current keyValue
                        groupAndContentMap.put(currentKey, entry.getValue());
                    }

//                        if (!groupAndContentMap.isEmpty() && (!prevEntry.get().getKey().isPartOfBong(currentKey) || !hasNext)) {
                    if (!groupAndContentMap.isEmpty() && (!isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentKey) || !hasNext)) {
                        // handle map entries
                        entrySetCallback.accept(groupAndContentMap);

                        if (groupCount.incrementAndGet() % 10000 == 0) {
                            LOG.info("Postgres - Produced Group Count: {}", groupCount.get());
                        }

                        // reset map
                        groupAndContentMap.clear(); // bong.isEmpty=guard against "on next item" in loop causes empty map
                    }

                    // move marker to next
                    prevEntry.set(entry);
                });
            }
            LOG.info("Postgres - Read Records Total: {}", readRecords.get());
            LOG.info("Postgres - Produced Group Total: {}", groupCount.get());
        }
    }

    /**
     * Produce Rawdata
     */
    protected void produce(BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        try (RawdataClient client = ProviderConfigurator.configure(targetConfiguration.asMap(), targetConfiguration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class)) {
            RawdataProducer producer = client.producer(targetConfiguration.evaluateToString("rawdata.topic"));

            final char[] encryptionKey = targetConfiguration.evaluateToString("rawdata.encryptionKey") != null ?
                    targetConfiguration.evaluateToString("rawdata.encryptionKey").toCharArray() : null;
            final byte[] encryptionSalt = targetConfiguration.evaluateToString("rawdata.encryptionSalt") != null ?
                    targetConfiguration.evaluateToString("rawdata.encryptionSalt").getBytes() : null;

            try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(1000, producer, encryptionKey, encryptionSalt)) {
                this.consume(entrySetMap -> {
                    StringBuilder itemLineBuffer = new StringBuilder();
                    itemLineBuffer.append(this.csvHeader()).append("\n");

                    AtomicReference<String> positionRef = new AtomicReference<>();

                    for (Map.Entry<T, String> entry : entrySetMap.entrySet()) {
                        if (positionRef.get() == null) {
                            positionRef.set(entry.getKey().toPosition());
                        }
                        itemLineBuffer.append(entry.getValue()).append("\n");
                    }

                    // buffered publisher
                    RawdataMessage.Builder messageBuilder = producer.builder();
                    messageBuilder.position(positionRef.get());
                    messageBuilder.put("entry", itemLineBuffer.toString().getBytes(StandardCharsets.UTF_8));
                    bufferedProducer.produce(messageBuilder.build());
                    //LOG.trace("{}\n{}", positionRef.get(), itemLineBuffer);
                }, isPrevKeyPartOfCurrentKey);
            }

            Path targetLocalTempPath = Paths.get(targetConfiguration.evaluateToString("local-temp-folder") + "/" + targetConfiguration.evaluateToString("rawdata.topic"));
            if (targetLocalTempPath.toFile().exists()) {
                LOG.info("Avro-file Count: {}: {}\n\t{}", targetLocalTempPath.toString(), Files.list(targetLocalTempPath).count(),
                        Files.walk(targetLocalTempPath).filter(Files::isRegularFile).map(Path::toString).collect(Collectors.joining("\n\t")));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
