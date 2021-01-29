package no.ssb.dc.collection.api.target;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.source.BufferedReadWrite;
import no.ssb.dc.collection.api.source.RepositoryKey;
import no.ssb.dc.collection.api.utils.EncodingUtils;
import no.ssb.dc.collection.api.utils.FixedThreadPool;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.dc.collection.api.worker.JsonParser;
import no.ssb.dc.collection.api.worker.MetadataContent;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public class BufferedRawdataProducer implements AutoCloseable {

    static final Logger LOG = LoggerFactory.getLogger(BufferedRawdataProducer.class);

    public static final AtomicLong readRecords = new AtomicLong(0);
    public static final AtomicLong recordCount = new AtomicLong(0);
    public static final AtomicLong groupCount = new AtomicLong(0);

    final FixedThreadPool threadPool;
    final LinkedBlockingDeque<RawdataMessage> queue;
    final RawdataProducer rawdataProducer;
    final EncryptionClient encryptionClient;
    final byte[] secretKey;
    final ByteBuffer encodingBuffer;
    final CsvSpecification specification;

    public BufferedRawdataProducer(SourceConfiguration sourceConfiguration, TargetConfiguration targetConfiguration, CsvSpecification specification, RawdataProducer rawdataProducer) {
        this.specification = specification;
        Objects.requireNonNull(rawdataProducer);
        this.rawdataProducer = rawdataProducer;
        threadPool = FixedThreadPool.newInstance();
        queue = new LinkedBlockingDeque<>(targetConfiguration.queueBufferSize());

        final char[] encryptionKey = targetConfiguration.hasRawdataEncryptionKey() ? targetConfiguration.rawdataEncryptionKey().toCharArray() : null;
        final byte[] encryptionSalt = targetConfiguration.hasRawdataEncryptionSalt() ? targetConfiguration.rawdataEncryptionSalt().getBytes() : null;

        this.encryptionClient = new EncryptionClient();

        if (encryptionKey != null && encryptionKey.length > 0 && encryptionSalt != null && encryptionSalt.length > 0) {
            this.secretKey = encryptionClient.generateSecretKey(encryptionKey, encryptionSalt).getEncoded();
            Arrays.fill(encryptionKey, (char) 0);
            Arrays.fill(encryptionSalt, (byte) 0);
        } else {
            this.secretKey = null;
        }

        this.encodingBuffer = ByteBuffer.allocateDirect(sourceConfiguration.queueValueBufferSize());
    }

    public enum RecordType {
        SINGLE,
        COLLECTION
    }

    public <K extends RepositoryKey> void readDatabaseAndProduceRawdata(BufferedReadWrite bufferedReadWrite, Class<K> keyClass, BiPredicate<K, K> isPrevKeyPartOfCurrentKey) {
        Objects.requireNonNull(specification);
        Map<String, String> metadataMap = new LinkedHashMap<>();
        bufferedReadWrite.readHeader((entry, hasNext) -> {
            metadataMap.put(entry.getKey(), entry.getValue());
        });

        String filepath = metadataMap.get("filepath");
        String filename = metadataMap.get("filename");
        List<String> csvHeaders = EncodingUtils.decodeArray(metadataMap.get("csvHeader"), encodingBuffer);
        String delimiterString = metadataMap.get("delimiter");
        List<String> avroHeader = EncodingUtils.decodeArray(metadataMap.get("avroHeader"), encodingBuffer);

        Objects.requireNonNull(csvHeaders);
        Objects.requireNonNull(delimiterString);
        Objects.requireNonNull(avroHeader);

        if (csvHeaders.size() != avroHeader.size()) {
            throw new IllegalStateException(String.format("CsvHeader and AvroHeader columns does not match (%s/%s)!", csvHeaders.size(), avroHeader.size()));
        }

        Map<String, Map.Entry<Integer, String>> headersMap = new LinkedHashMap<>();
        for (int i = 0; i < csvHeaders.size(); i++) {
            headersMap.put(csvHeaders.get(i), Map.entry(i, avroHeader.get(i)));
        }

        if (specification.columns.groupByKeys().isEmpty()) {
            readBufferedRecordAndProduceRawdataMessage(bufferedReadWrite, keyClass,
                    recordSetMap -> produceRawdataMessage(RecordType.SINGLE, filepath, filename, headersMap, delimiterString, recordSetMap));
        } else {
            readBufferedRecordThenGroupAndProduceRawdataMessage(bufferedReadWrite, keyClass, isPrevKeyPartOfCurrentKey,
                    recordSetMap -> produceRawdataMessage(RecordType.COLLECTION, filepath, filename, headersMap, delimiterString, recordSetMap));
        }
    }

    <K extends RepositoryKey> void readBufferedRecordAndProduceRawdataMessage(BufferedReadWrite bufferedReadWrite,
                                                                              Class<K> keyClass,
                                                                              Consumer<Map<K, String>> entrySetCallback) {
        AtomicLong handledRecords = new AtomicLong();

        bufferedReadWrite.readRecord(keyClass, (currentEntry, hasNext) -> {
            if (readRecords.incrementAndGet() % 100000 == 0) {
                LOG.info("Database - Read Records: {}", readRecords.get());
            }

            Map<K, String> recordKeyAndContentMap = new LinkedHashMap<>();
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

    <K extends RepositoryKey> void readBufferedRecordThenGroupAndProduceRawdataMessage(BufferedReadWrite bufferedReadWrite,
                                                                                       Class<K> keyClass,
                                                                                       BiPredicate<K, K> isPrevKeyPartOfCurrentKey,
                                                                                       Consumer<Map<K, String>> entrySetCallback) {
        AtomicReference<Map.Entry<K, String>> prevEntry = new AtomicReference<>();
        Map<K, String> groupByKeyAndContentMap = new LinkedHashMap<>();
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
            } else if (isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentEntry.getKey())) {
                groupByKeyAndContentMap.put(currentEntry.getKey(), currentEntry.getValue());
                prevEntry.set(currentEntry);

                // the previous key and current key DOES NOT match - commit group or is last entry
            } else if (!isPrevKeyPartOfCurrentKey.test(prevEntry.get().getKey(), currentEntry.getKey())) {
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

    private <K extends RepositoryKey> void commitAndClearGroup(Consumer<Map<K, String>> entrySetCallback,
                                                               Map<K, String> groupByKeyAndContentMap,
                                                               AtomicLong handledRecords) {
        entrySetCallback.accept(groupByKeyAndContentMap);

        handledRecords.addAndGet(groupByKeyAndContentMap.values().size());

        if (groupCount.incrementAndGet() % 10000 == 0) {
            LOG.info("Database - Produced Group Count: {}", groupCount.get());
        }

        // reset map
        groupByKeyAndContentMap.clear();
    }


    public <K extends RepositoryKey> void produceRawdataMessage(RecordType recordType,
                                                                String filepath,
                                                                String filename,
                                                                Map<String, Map.Entry<Integer, String>> headersMap,
                                                                String delimiterString, Map<K, String> recordSetMap) {
        Objects.requireNonNull(specification);
        StringBuilder itemLineBuffer = new StringBuilder();
        String avroHeader = String.join(delimiterString, headersMap.keySet());
        itemLineBuffer.append(avroHeader).append("\n");

        AtomicReference<String> positionRef = new AtomicReference<>();

        // read record
        for (Map.Entry<K, String> entry : recordSetMap.entrySet()) {
            // use first record to get position and header mapping
            if (positionRef.get() == null) {
                positionRef.set(entry.getKey().toPosition());

            }
            itemLineBuffer.append(entry.getValue()).append("\n");
        }
        byte[] entryData = itemLineBuffer.toString().getBytes(StandardCharsets.UTF_8);

        // create metadata - also see: CsvReader
        MetadataContent.Builder metadataContent = new MetadataContent.Builder()
                .topic(rawdataProducer.topic())
                .position(positionRef.get())
                .resourceType("entry")
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
                .sourceCharset(specification.fileDescriptor.charset.displayName())
                .delimiter(delimiterString)
                .recordType(recordType.name().toLowerCase());

        for (Map.Entry<String, Map.Entry<Integer, String>> entry : headersMap.entrySet()) {
            metadataContent.csvMapping(entry.getKey(), entry.getValue().getValue());
        }

        // buffered publisher - copyMessage() encrypts all data
        RawdataMessage.Builder messageBuilder = rawdataProducer.builder();
        messageBuilder.position(positionRef.get());
        messageBuilder.put("entry", entryData);
        messageBuilder.put("manifest.json", JsonParser.createJsonParser().toJSON(metadataContent.build().getElementNode()).getBytes());
        produce(messageBuilder.build());
        //LOG.trace("{}\n{}", positionRef.get(), itemLineBuffer);
    }

    public void produce(RawdataMessage message) {
        if (!queue.offer(message)) {
            commitQueue();

            // try add violated capacity element
            if (!queue.add(message)) {
                throw new RuntimeException("Unable to queue message after publishing queue buffers");
            }
        }
    }

    void commitQueue() {
        List<CompletableFuture<String>> futureList = new ArrayList<>();
        List<String> publishPositionList = new ArrayList<>();
        RawdataMessage rawdataMessage;
        while ((rawdataMessage = queue.poll()) != null) {
            publishPositionList.add(rawdataMessage.position());
            AtomicReference<RawdataMessage> rawdataMessageRef = new AtomicReference<>(rawdataMessage);
            futureList.add(CompletableFuture.supplyAsync(() -> {
                RawdataMessage.Builder copyOfRawadataMessage = copyMessage(rawdataMessageRef.get());
                rawdataProducer.buffer(copyOfRawadataMessage);
                return copyOfRawadataMessage.position();
            }, threadPool.getExecutor()));
        }

        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    //LOG.info("Publish: {}", publishPositionList);
                    rawdataProducer.publish(publishPositionList.toArray(new String[0]));
                    return v;
                })
                .exceptionally(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                })
                .join();
    }

    RawdataMessage.Builder copyMessage(RawdataMessage sourceMessage) {
        RawdataMessage.Builder copyMessageBuilder = rawdataProducer.builder();
        copyMessageBuilder.position(sourceMessage.position());
        copyMessageBuilder.ulid(sourceMessage.ulid());
        for (String key : sourceMessage.keys()) {
            copyMessageBuilder.put(key, tryEncryptContent(sourceMessage.get(key)));
        }
        return copyMessageBuilder;
    }

    private byte[] tryEncryptContent(byte[] content) {
        if (secretKey != null) {
            byte[] iv = encryptionClient.generateIV();
            return encryptionClient.encrypt(secretKey, iv, content);
        }
        return content;
    }


    @Override
    public void close() {
        commitQueue();
        threadPool.shutdownAndAwaitTermination();
    }
}
