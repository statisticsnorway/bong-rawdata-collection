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

public class FagkodeWorker implements CsvWorker<FagkodeKey> {

    private final LmdbCsvRepository<FagkodeKey> csvRepository;

    public FagkodeWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                FagkodeKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                FagkodeWorker.csvHeader(),
                FagkodeKey::isPartOf
        );
    }

    // fagkoder_nyeoggamle_2019.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("FagKode;FagNavn;HarStandpunkt;Eksamensform;ForsteEksamen;SisteEksamen;ForsteUndervisning;SisteUndervisning", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Fagkode", ConversionUtils.toString(record.tokens.get(0)));
            return GenericKey.create(FagkodeKey.class, values);
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
