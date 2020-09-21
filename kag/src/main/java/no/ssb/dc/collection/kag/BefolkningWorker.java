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

public class BefolkningWorker implements CsvWorker<BefolkningKey> {

    private final LmdbCsvRepository<BefolkningKey> csvRepository;

    public BefolkningWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                BefolkningKey.class,
                "\\;",
                StandardCharsets.US_ASCII,
                BefolkningWorker.csvHeader(),
                BefolkningKey::equals
        );
    }

    // sitbef_g2019m09d30.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("snr;FNR;foedselsaar;alder;KJOENN;status;status_dnr;statborg;KOMMNR;mor_snr;mor_fnr;far_snr;far_fnr;fodeland;landbak;invkat;fodato;fraland;fralanddato;botid", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Fnr", ConversionUtils.toLong(record.tokens.get(1)));
            return GenericKey.create(BefolkningKey.class, values);
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
