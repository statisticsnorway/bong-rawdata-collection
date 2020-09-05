package no.ssb.dc.bong.ng.repository;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.csv.CSVReader;
import no.ssb.dc.bong.commons.lmdb.LmdbBufferedWriter;
import no.ssb.dc.bong.commons.lmdb.LmdbEnvironment;
import no.ssb.dc.bong.commons.rawdata.BufferedRawdataProducer;
import no.ssb.dc.bong.commons.rawdata.ConversionUtils;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class NGLmdbBongRepository {

    static final Logger LOG = LoggerFactory.getLogger(NGLmdbBongRepository.class);

    static final AtomicLong csvReadLines = new AtomicLong(0);
    static final AtomicLong bongReadRecords = new AtomicLong(0);
    static final AtomicLong bongCount = new AtomicLong(0);

    private final DynamicConfiguration sourceConfiguration;
    private final DynamicConfiguration targetConfiguration;

    public NGLmdbBongRepository(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        this.sourceConfiguration = sourceConfiguration.asDynamicConfiguration();
        this.targetConfiguration = targetConfiguration.asDynamicConfiguration();
    }

    public void buildDatabase() {
        String dateFormat = sourceConfiguration.evaluateToString("csv.dateFormat");
        Objects.requireNonNull(dateFormat);
        CSVReader csvReader = new CSVReader(sourceConfiguration, true);
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, true)) {
            try (LmdbBufferedWriter lmdbBufferedWriter = new LmdbBufferedWriter(sourceConfiguration, lmdbEnvironment)) {
                // loop entries in file and write records
                CompletableFuture<Void> futureReader = CompletableFuture.runAsync(() -> {
                    csvReader.parse("\\|", StandardCharsets.ISO_8859_1, record -> {
                        long loc = ConversionUtils.toLong(record.tokens.get(0));
                        int bong = ConversionUtils.toInteger(record.tokens.get(2));
                        Date ts = ConversionUtils.toDate(record.tokens.get(1), dateFormat);
                        lmdbBufferedWriter.writeRecord(new NGBongKey(record.filename, loc, bong, ts.getTime()), record.line);
                        if (csvReadLines.incrementAndGet() % 100000 == 0) {
                            LOG.info("Source - Read lines: {}", csvReadLines.get());
                        }
                    });
                }).exceptionally(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    } else {
                        throw new RuntimeException(throwable);
                    }
                });
                futureReader.join();
            }
        }
        LOG.info("Source - Read Lines Total: {}", csvReadLines.get());
    }

    void readDatabase(Consumer<Map<NGBongKey, String>> bongCallback) {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(sourceConfiguration, false)) {
            try (LmdbBufferedWriter lmdbBufferedWriter = new LmdbBufferedWriter(sourceConfiguration, lmdbEnvironment)) {

                CompletableFuture<Void> futureReader = CompletableFuture.runAsync(() -> {
                    AtomicReference<Map.Entry<NGBongKey, String>> prevEntry = new AtomicReference<>();
                    Map<NGBongKey, String> bongGroupAndContentMap = new LinkedHashMap<>();

                    lmdbBufferedWriter.readRecord(NGBongKey.class, (entry, hasNext) -> {
                        if (bongReadRecords.incrementAndGet() % 100000 == 0) {
                            LOG.info("Lmdb - Read Records: {}", bongReadRecords.get());
                        }
                        NGBongKey NGBongKey = entry.getKey();

                        // mark first position
                        if (prevEntry.get() == null) {
                            prevEntry.set(entry);
                            return;
                        }

                        if (prevEntry.get().getKey().isPartOfBong(NGBongKey)) {
                            // add previous keyValue if absent
                            if (!bongGroupAndContentMap.containsKey(prevEntry.get().getKey())) {
                                bongGroupAndContentMap.put(prevEntry.get().getKey(), prevEntry.get().getValue());
                            }

                            // add current keyValue
                            bongGroupAndContentMap.put(NGBongKey, entry.getValue());
                        }

                        if (!bongGroupAndContentMap.isEmpty() && (!prevEntry.get().getKey().isPartOfBong(NGBongKey) || !hasNext)) {
                            // handle map entries
                            bongCallback.accept(bongGroupAndContentMap);

                            if (bongCount.incrementAndGet() % 10000 == 0) {
                                LOG.info("Lmdb - Produced Bong Count: {}", bongReadRecords.get());
                            }

                            // reset map
                            bongGroupAndContentMap.clear(); // bong.isEmpty=guard against "on next item" in loop causes empty map
                        }

                        // move marker to next
                        prevEntry.set(entry);
                    });
                }).exceptionally(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    } else {
                        throw new RuntimeException(throwable);
                    }
                });
                futureReader.join();
            }
            LOG.info("Lmdb - Read Records Total: {}", bongReadRecords.get());
            LOG.info("Target - Produced Bong Total: {}", bongCount.get());
        }
    }

    public void produceRawdata() {
        final String CSV_HEADERS = "#AVSBUTIKKEANLOK_NR|KJOPS_DT_TD|BONGNUMMER|KASSEPOS_NR|KASSETYPE|MOMS_BEL|TOTALRABATT_BEL" +
                "|VAREKJOP_UTEN_PANT_BEL|VAREKJOP_MED_PANT_BEL|VARELINJER_ANT|BRUTTOVARELINJE_BEL|VARERABATT_BEL|VAREANTVEKT|VAREEAN_NR|VAREENHETKODE_NR|VARENAVN|MVASATS";

        try (RawdataClient client = ProviderConfigurator.configure(targetConfiguration.asMap(), targetConfiguration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class)) {
            RawdataProducer producer = client.producer(targetConfiguration.evaluateToString("rawdata.topic"));

            final char[] encryptionKey = targetConfiguration.evaluateToString("rawdata.encryptionKey") != null ?
                    targetConfiguration.evaluateToString("rawdata.encryptionKey").toCharArray() : null;
            final byte[] encryptionSalt = targetConfiguration.evaluateToString("rawdata.encryptionSalt") != null ?
                    targetConfiguration.evaluateToString("rawdata.encryptionSalt").getBytes() : null;

            try (BufferedRawdataProducer bufferedProducer = new BufferedRawdataProducer(1000, producer, encryptionKey, encryptionSalt)) {
                this.readDatabase(bongMap -> {
                    StringBuilder itemLineBuffer = new StringBuilder();
                    itemLineBuffer.append(CSV_HEADERS).append("\n");

                    AtomicReference<String> positionRef = new AtomicReference<>();

                    for (Map.Entry<NGBongKey, String> entry : bongMap.entrySet()) {
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
