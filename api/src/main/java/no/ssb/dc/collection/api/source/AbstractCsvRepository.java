package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.csv.CsvReader;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.dc.collection.api.worker.JsonParser;
import no.ssb.dc.collection.api.worker.MetadataContent;
import no.ssb.rawdata.api.RawdataMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract public class AbstractCsvRepository<T extends RepositoryKey> implements AutoCloseable {

    static final Logger LOG = LoggerFactory.getLogger(CsvWorker.class);

    public static final AtomicLong csvReadLines = new AtomicLong(0);
    public static final AtomicLong readRecords = new AtomicLong(0);
    public static final AtomicLong recordCount = new AtomicLong(0);
    public static final AtomicLong groupCount = new AtomicLong(0);

    protected final SourceConfiguration sourceConfiguration;
    protected final TargetConfiguration targetConfiguration;
    protected final CsvSpecification specification;
    protected final Class<T> keyClass;
    protected final BiPredicate<T, T> isPrevKeyPartOfCurrentKey;
    private final ByteBuffer encodingBuffer;
    private final Base64.Encoder base64encoder;
    private final Base64.Decoder base64decoder;

    public AbstractCsvRepository(SourceConfiguration sourceConfiguration,
                                 TargetConfiguration targetConfiguration,
                                 CsvSpecification specification,
                                 Class<T> keyClass,
                                 BiPredicate<T, T> isPrevKeyPartOfCurrentKey) {
        this.sourceConfiguration = sourceConfiguration;
        this.targetConfiguration = targetConfiguration;
        this.specification = specification;
        this.keyClass = keyClass;
        this.isPrevKeyPartOfCurrentKey = isPrevKeyPartOfCurrentKey;
        int valueSize = sourceConfiguration.hasQueueValueBufferSize() ? 2048 : sourceConfiguration.queueValueBufferSize();
        this.encodingBuffer = ByteBuffer.allocateDirect(valueSize);
        this.base64encoder = Base64.getEncoder();
        this.base64decoder = Base64.getDecoder();
    }

    abstract public void prepare(Function<CsvParser.Record, T> produceSortableKey);

    abstract public void consume(Consumer<Map<T, String>> entrySetCallback);

    abstract public void produce();

    protected void readCsvFileAndPrepareDatabase(CsvReader csvReader, BufferedReadWrite bufferedReadWrite, Function<CsvParser.Record, T> produceSortableKey) {
        AtomicBoolean isHeaderRecord = new AtomicBoolean(true);
        // this call do process multiple csv files
        csvReader.parse(specification.fileDescriptor.delimiter, specification.fileDescriptor.charset, record -> {
            // all files must comply with the same header, so we persist it once
            if (isHeaderRecord.get()) {
                String headerLine = record.asHeader();
                bufferedReadWrite.writeHeader("filepath", record.filepath);
                bufferedReadWrite.writeHeader("filename", record.filename);
                bufferedReadWrite.writeHeader("csvHeader", encodeArray(new ArrayList<>(record.headers.keySet())));
                String delimiterString = Character.toString(record.delimiter);
                bufferedReadWrite.writeHeader("delimiter", delimiterString);
                bufferedReadWrite.writeHeader("avroHeader", encodeArray(record.headers.values().stream().map(Map.Entry::getValue).collect(Collectors.toList())));
                isHeaderRecord.set(false);
            }

            RepositoryKey sortableGroupKey = produceSortableKey.apply(record); // worker produces valid groupKey (CsvDynamicWorker.createDynamicKey produces a toBuffer(key))

            LOG.trace("{}: {}\n\t{}", csvReadLines.get(), sortableGroupKey, record.asLine());
            bufferedReadWrite.writeRecord(sortableGroupKey, record.asLine());

            if (csvReadLines.incrementAndGet() % 100000 == 0) {
                LOG.info("Source - Read lines: {}", csvReadLines.get());
            }
        });

        bufferedReadWrite.close();

        LOG.info("Source - Read Lines Total: {}", csvReadLines.get());
    }

    String encodeArray(List<String> tokens) {
        try {
            for (String segment : tokens) {
                byte[] bytes = segment.getBytes(StandardCharsets.UTF_8);
                encodingBuffer.putInt(bytes.length);
                encodingBuffer.put(bytes);
            }
            encodingBuffer.flip();

            byte[] array = new byte[encodingBuffer.remaining()];
            encodingBuffer.get(array);
            return base64encoder.encodeToString(array);
        } finally {
            encodingBuffer.clear();
        }
    }

    List<String> decodeArray(String base64) {
        List<String> list = new ArrayList<>();
        byte[] encodedBytes = base64decoder.decode(base64);
        try {
            encodingBuffer.put(encodedBytes);
            encodingBuffer.flip();
            for (; ; ) {
                int offset = encodingBuffer.getInt();
                if (offset == 0) break;
                byte[] segment = new byte[offset];
                encodingBuffer.get(segment);
                list.add(new String(segment));
                if (!encodingBuffer.hasRemaining()) {
                    break;
                }
            }
        } finally {
            encodingBuffer.clear();
        }
        return list;
    }

    protected Map<String, String> readDatabaseMeta(BufferedReadWrite bufferedReadWrite) {
        Map<String, String> metaMap = new LinkedHashMap<>();
        bufferedReadWrite.readHeader((entry, hasNext) -> {
            metaMap.put(entry.getKey(), entry.getValue());
        });
        return metaMap;
    }

    // TODO to be used when there is no groupBy applied
    protected void readDatabaseAndHandleRecord(BufferedReadWrite bufferedReadWrite, Consumer<Map<T, String>> entrySetCallback) {
        AtomicLong handledRecords = new AtomicLong();

        bufferedReadWrite.readRecord(keyClass, (currentEntry, hasNext) -> {
            if (readRecords.incrementAndGet() % 100000 == 0) {
                LOG.info("Database - Read Records: {}", readRecords.get());
            }

            Map<T, String> recordKeyAndContentMap = new LinkedHashMap<>();
            recordKeyAndContentMap.put(currentEntry.getKey(), currentEntry.getValue());
            entrySetCallback.accept(recordKeyAndContentMap);
            handledRecords.incrementAndGet();

            if (recordCount.incrementAndGet() % 10000 == 0) {
                LOG.info("Database - Produced Record Count: {}", recordCount.get());
            }
        });

        LOG.debug("Read: {} -- Handled: {}", readRecords.get(), handledRecords.get());

        bufferedReadWrite.close();

        LOG.info("Database - Read Records Total: {}", readRecords.get());
        LOG.info("Database - Produced Records Total: {}", recordCount.get());
    }

    protected void readDatabaseAndHandleGroup(BufferedReadWrite bufferedReadWrite, Consumer<Map<T, String>> entrySetCallback) {
        AtomicReference<Map.Entry<T, String>> prevEntry = new AtomicReference<>();
        Map<T, String> groupByKeyAndContentMap = new LinkedHashMap<>();
        AtomicLong handledRecords = new AtomicLong();
        bufferedReadWrite.readRecord(keyClass, (currentEntry, hasNext) -> {
            if (readRecords.incrementAndGet() % 100000 == 0) {
                LOG.info("Database - Read Records: {}", readRecords.get());
            }

            // new group collection
            if (groupByKeyAndContentMap.isEmpty()) {
                groupByKeyAndContentMap.put(currentEntry.getKey(), currentEntry.getValue());
                prevEntry.set(currentEntry);

                // the previous key and current key matches, add currentEntry to group
            } else if (this.isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentEntry.getKey())) {
                groupByKeyAndContentMap.put(currentEntry.getKey(), currentEntry.getValue());
                prevEntry.set(currentEntry);

                // the previous key and current key DOES NOT match - commit group or is last entry
            } else if (!this.isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentEntry.getKey())) {
                commitAndClearGroup(entrySetCallback, groupByKeyAndContentMap, handledRecords);
                groupByKeyAndContentMap.put(currentEntry.getKey(), currentEntry.getValue());
                prevEntry.set(currentEntry);
            }

            // handle tail
            if (!hasNext) {
                groupByKeyAndContentMap.putIfAbsent(currentEntry.getKey(), currentEntry.getValue());
                commitAndClearGroup(entrySetCallback, groupByKeyAndContentMap, handledRecords);
            }
        });

        LOG.debug("Read: {} -- Handled: {}", readRecords.get(), handledRecords.get());

        bufferedReadWrite.close();

        LOG.info("Database - Read Records Total: {}", readRecords.get());
        LOG.info("Database - Produced Group Total: {}", groupCount.get());
    }

    private void commitAndClearGroup(Consumer<Map<T, String>> entrySetCallback, Map<T, String> groupByKeyAndContentMap, AtomicLong handledRecords) {
        entrySetCallback.accept(groupByKeyAndContentMap);

        handledRecords.addAndGet(groupByKeyAndContentMap.values().size());

        if (groupCount.incrementAndGet() % 10000 == 0) {
            LOG.info("Database - Produced Group Count: {}", groupCount.get());
        }

        // reset map
        groupByKeyAndContentMap.clear();
    }

    protected void readDatabaseAndProduceRawdata(BufferedReadWrite bufferedReadWrite, BufferedRawdataProducer bufferedProducer) {
        Map<String, String> metadataMap = readDatabaseMeta(bufferedReadWrite);

        String filepath = metadataMap.get("filepath");
        String filename = metadataMap.get("filename");
        List<String> csvHeaders = decodeArray(metadataMap.get("csvHeader"));
        String delimiterString = metadataMap.get("delimiter");
        List<String> avroHeader = decodeArray(metadataMap.get("avroHeader"));

        Objects.requireNonNull(csvHeaders);
        Objects.requireNonNull(delimiterString);
        Objects.requireNonNull(avroHeader);

        if (csvHeaders.size() != avroHeader.size()) {
            throw new IllegalStateException(String.format("CsvHeader and AvroHeader columns does not match (%s/%s)!", csvHeaders.size(), avroHeader.size()));
        }

        Map<String, Map.Entry<Integer, String>> headersMap = new LinkedHashMap<>();
        int n = 0;
        for (String csvHeader : csvHeaders) {
            headersMap.put(csvHeader, Map.entry(n, avroHeader.get(n)));
            n++;
        }

        this.readDatabaseAndHandleGroup(bufferedReadWrite, recordSetMap -> produceRawdataMessage(bufferedProducer, filepath, filename, headersMap, delimiterString, recordSetMap));
    }

    protected void produceRawdataMessage(BufferedRawdataProducer bufferedProducer, String filepath, String filename, Map<String, Map.Entry<Integer, String>> headersMap, String delimiterString, Map<T, String> recordSetMap) {
        StringBuilder itemLineBuffer = new StringBuilder();
        String avroHeader = String.join(delimiterString, headersMap.keySet());
        itemLineBuffer.append(avroHeader).append("\n");

        AtomicReference<String> positionRef = new AtomicReference<>();

        // read record
        for (Map.Entry<T, String> entry : recordSetMap.entrySet()) {
            // use first record to get position and header mapping
            if (positionRef.get() == null) {
                positionRef.set(entry.getKey().toPosition());

            }
            itemLineBuffer.append(entry.getValue()).append("\n");
        }
        byte[] entryData = itemLineBuffer.toString().getBytes(StandardCharsets.UTF_8);

        // create metadata - also see: CsvReader
        MetadataContent.Builder metadataContent = new MetadataContent.Builder()
                .topic(bufferedProducer.producer().topic())
                .position(positionRef.get())
                .contentKey("entry")
                .source(specification.metadata.source)
                .dataset(specification.metadata.dataset)
                .tag(specification.metadata.tag)
                .description(specification.metadata.description)
                .charset(StandardCharsets.UTF_8.displayName())
                .contentType(specification.fileDescriptor.contentType)
                .contentLength(entryData.length)
                .markCreatedDate();

        // store csv/avro mapping
        metadataContent
                .sourcePath(filepath)
                .sourceFile(filename)
                .delimiter(delimiterString)
                .sourceCharset(specification.fileDescriptor.charset.displayName());

        for (Map.Entry<String, Map.Entry<Integer, String>> entry : headersMap.entrySet()) {
            metadataContent.csvMapping(entry.getKey(), entry.getValue().getValue());
        }

        // buffered publisher
        RawdataMessage.Builder messageBuilder = bufferedProducer.producer().builder();
        messageBuilder.position(positionRef.get());
        messageBuilder.put("entry", entryData);
        messageBuilder.put("manifest.json", JsonParser.createJsonParser().toJSON(metadataContent.build().getElementNode()).getBytes());
        bufferedProducer.produce(messageBuilder.build());
        //LOG.trace("{}\n{}", positionRef.get(), itemLineBuffer);
    }

}
