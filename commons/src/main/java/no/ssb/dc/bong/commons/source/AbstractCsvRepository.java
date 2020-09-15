package no.ssb.dc.bong.commons.source;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.AbstractConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.csv.CSVReader;
import no.ssb.dc.bong.commons.target.BufferedRawdataProducer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

abstract public class AbstractCsvRepository<T extends RepositoryKey> {

    static final Logger LOG = LoggerFactory.getLogger(CsvWorker.class);

    public static final AtomicLong csvReadLines = new AtomicLong(0);
    public static final AtomicLong readRecords = new AtomicLong(0);
    public static final AtomicLong groupCount = new AtomicLong(0);

    protected final DynamicConfiguration sourceConfiguration;
    protected final DynamicConfiguration targetConfiguration;
    protected final Class<T> keyClass;
    protected final String delimeter;
    protected final Charset csvCharset;
    protected final String csvHeader;
    protected final BiPredicate<T, T> isPrevKeyPartOfCurrentKey;

    public AbstractCsvRepository(AbstractConfiguration sourceConfiguration,
                                 TargetConfiguration targetConfiguration,
                                 Class<T> keyClass,
                                 String delimeter,
                                 Charset csvCharset,
                                 String csvHeader,
                                 BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        this.sourceConfiguration = sourceConfiguration.asDynamicConfiguration();
        this.targetConfiguration = targetConfiguration.asDynamicConfiguration();
        this.keyClass = keyClass;
        this.delimeter = delimeter;
        this.csvCharset = csvCharset;
        this.csvHeader = csvHeader;
        this.isPrevKeyPartOfCurrentKey = isPrevKeyPartOfCurrentKey;
    }

    protected void readCsvFileAndPrepareDatabase(CSVReader csvReader, BufferedReadWrite bufferedReadWrite, Function<CSVReader.Record, T> produceSortableKey) {
        csvReader.parse(delimeter, csvCharset, record -> {
            RepositoryKey key = produceSortableKey.apply(record);

            bufferedReadWrite.writeRecord(key, record.line);

            if (csvReadLines.incrementAndGet() % 100000 == 0) {
                LOG.info("Source - Read lines: {}", csvReadLines.get());
            }
        });

        bufferedReadWrite.close();

        LOG.info("Source - Read Lines Total: {}", csvReadLines.get());
    }

    protected void readDatabaseAndHandleGroup(BufferedReadWrite bufferedReadWrite, Consumer<Map<T, String>> entrySetCallback) {
        AtomicReference<Map.Entry<T, String>> prevEntry = new AtomicReference<>();
        Map<T, String> groupAndContentMap = new LinkedHashMap<>();

        bufferedReadWrite.readRecord(keyClass, (entry, hasNext) -> {
            if (readRecords.incrementAndGet() % 100000 == 0) {
                LOG.info("Database - Read Records: {}", readRecords.get());
            }
            T currentKey = entry.getKey();

            // mark first position
            if (prevEntry.get() == null) {
                prevEntry.set(entry);
                return;
            }

            if (isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentKey)) {
                // add previous keyValue if absent
                if (!groupAndContentMap.containsKey(prevEntry.get().getKey())) {
                    groupAndContentMap.put(prevEntry.get().getKey(), prevEntry.get().getValue());
                }

                // add current keyValue
                groupAndContentMap.put(currentKey, entry.getValue());
            }

            if (!groupAndContentMap.isEmpty() && (!isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentKey) || !hasNext)) {
                // handle map entries
                entrySetCallback.accept(groupAndContentMap);

                if (groupCount.incrementAndGet() % 10000 == 0) {
                    LOG.info("Postgres - Produced Group Count: {}", groupCount.get());
                }

                // reset map
                groupAndContentMap.clear(); // group.isEmpty=guard against "on next item" in loop causes empty map
            }

            // move marker to next
            prevEntry.set(entry);
        });

        bufferedReadWrite.close();

        LOG.info("Database - Read Records Total: {}", readRecords.get());
        LOG.info("Database - Produced Group Total: {}", groupCount.get());
    }

    protected void readDatabaseAndProduceRawdata(BufferedReadWrite bufferedReadWrite, RawdataProducer producer, BufferedRawdataProducer bufferedProducer) {
        this.readDatabaseAndHandleGroup(bufferedReadWrite, entrySetMap -> {
            StringBuilder itemLineBuffer = new StringBuilder();
            itemLineBuffer.append(this.csvHeader).append("\n");

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
        });
    }

}
