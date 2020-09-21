package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.source.CsvWorker;
import no.ssb.dc.collection.api.source.GenericKey;
import no.ssb.dc.collection.api.source.LmdbCsvRepository;
import no.ssb.dc.collection.api.utils.ULIDGenerator;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class GsiWorker implements CsvWorker<GsiKey> {

    private final LmdbCsvRepository<GsiKey> csvRepository;

    public GsiWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                GsiKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                GsiWorker.csvHeader(),
                GsiKey::equals
        );
    }

    // gsi_2019.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("orgnrbed;navn;kommnr", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("ulid", ULIDGenerator.toUUID(ULIDGenerator.generate()).toString());
            return GenericKey.create(GsiKey.class, values);
        });
    }

    @Override
    public void produce() {
        csvRepository.produce();
    }

    @Override
    public void close() {
        csvRepository.close();
    }
}
