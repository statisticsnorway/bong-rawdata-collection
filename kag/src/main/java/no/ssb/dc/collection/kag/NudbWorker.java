package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.source.CsvWorker;
import no.ssb.dc.collection.api.source.GenericKey;
import no.ssb.dc.collection.api.source.LmdbCsvRepository;
import no.ssb.dc.collection.api.utils.ConversionUtils;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class NudbWorker implements CsvWorker<NudbKey> {

    private final LmdbCsvRepository<NudbKey> csvRepository;

    public NudbWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                NudbKey.class,
                "\\;",
                StandardCharsets.US_ASCII,
                NudbWorker.csvHeader(),
                NudbKey::equals
        );
    }

    // nudbfil_2019.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("SNR_NUDB;NUS2000_FAR_16;NUS2000_MOR_16;SOSBAK", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("SnrNudb", ConversionUtils.toString(record.tokens.get(0)));
            return GenericKey.create(NudbKey.class, values);
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
