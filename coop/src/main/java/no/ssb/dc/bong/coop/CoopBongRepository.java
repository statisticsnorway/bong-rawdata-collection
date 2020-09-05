package no.ssb.dc.bong.coop;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.csv.CSVReader;
import no.ssb.dc.bong.commons.lmdb.LmdbBufferedWriter;
import no.ssb.dc.bong.commons.lmdb.LmdbEnvironment;
import no.ssb.dc.bong.commons.rawdata.ConversionUtils;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CoopBongRepository {

    static final Logger LOG = LoggerFactory.getLogger(CoopBongRepository.class);

    private final DynamicConfiguration configuration;

    public CoopBongRepository(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    // "Dato|Butikk_nr|Bongnr|GTIN|Coop_varnenr|Vare|Dato_tid|Mengde|Enhet|Pris|Valuta"
    public void buildDatabase() {
        String dateFormat = configuration.evaluateToString("csv.dateFormat");
        Objects.requireNonNull(dateFormat);
        CSVReader csvReader = new CSVReader(configuration, false);
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, true)) {
            try (LmdbBufferedWriter lmdbBufferedWriter = new LmdbBufferedWriter(configuration, lmdbEnvironment)) {
                // loop entries in file and write records
                CompletableFuture<Void> futureReader = CompletableFuture.runAsync(() -> {
                    AtomicInteger counter = new AtomicInteger(0);
                    csvReader.parse("\\;", StandardCharsets.ISO_8859_1, record -> {
                        long loc = ConversionUtils.toLong(record.tokens.get(1));
                        int bong = ConversionUtils.toInteger(record.tokens.get(2));
                        Date ts = ConversionUtils.toDate(record.tokens.get(6), dateFormat); // 2018 10 05 180311 yyyyMMddhhmmss
                        lmdbBufferedWriter.writeRecord(new CoopBongKey(record.filename, loc, bong, ts.getTime()), record.line);
                        if (counter.incrementAndGet() % 10000 == 0) {
                            LOG.trace("Read lines: {}", counter.get());
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
    }

    void readDatabase(Consumer<Map<CoopBongKey, String>> bongCallback) {
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, false)) {
            try (LmdbBufferedWriter lmdbBufferedWriter = new LmdbBufferedWriter(configuration, lmdbEnvironment)) {

                CompletableFuture<Void> futureReader = CompletableFuture.runAsync(() -> {
                    AtomicReference<Map.Entry<CoopBongKey, String>> prevEntry = new AtomicReference<>();
                    Map<CoopBongKey, String> bongGroupAndContentMap = new LinkedHashMap<>();

                    lmdbBufferedWriter.readRecord(CoopBongKey.class, (entry, hasNext) -> {
                        CoopBongKey NGBongKey = entry.getKey();

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
        }
    }

    public void produceRawdata() {
        final String CSV_HEADERS = "Dato|Butikk_nr|Bongnr|GTIN|Coop_varnenr|Vare|Dato_tid|Mengde|Enhet|Pris|Valuta";

        try (RawdataClient client = ProviderConfigurator.configure(Map.of(), configuration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class)) {
            RawdataProducer producer = client.producer(configuration.evaluateToString("rawdata.topic"));

            this.readDatabase(bongMap -> {
                StringBuilder itemLineBuffer = new StringBuilder();
                itemLineBuffer.append(CSV_HEADERS).append("\n");

                AtomicReference<String> positionRef = new AtomicReference<>();

                for (Map.Entry<CoopBongKey, String> entry : bongMap.entrySet()) {
                    if (positionRef.get() == null) {
                        positionRef.set(entry.getKey().toPosition());
                    }
                    itemLineBuffer.append(entry.getValue()).append("\n");
                }

                // todo make a buffered publisher
                RawdataMessage.Builder messageBuilder = producer.builder();
                messageBuilder.position(positionRef.get());
                messageBuilder.put("entry", itemLineBuffer.toString().getBytes(StandardCharsets.UTF_8));
                producer.buffer(messageBuilder);
                LOG.trace("{}\n{}", positionRef.get(), itemLineBuffer);

                producer.publish(positionRef.get());
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
