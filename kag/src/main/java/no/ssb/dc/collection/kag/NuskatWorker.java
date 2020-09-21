package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.source.CsvWorker;
import no.ssb.dc.collection.api.source.GenericKey;
import no.ssb.dc.collection.api.source.LmdbCsvRepository;
import no.ssb.dc.collection.api.utils.ConversionUtils;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

public class NuskatWorker implements CsvWorker<NuskatKey> {

    private final LmdbCsvRepository<NuskatKey> csvRepository;

    public NuskatWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                NuskatKey.class,
                "\\;",
                Charset.forName("windows-1252"),
                NuskatWorker.csvHeader(),
                NuskatKey::equals
        );
    }

    // NUS2000.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("nus2000;tekstl;kodetype;lkltrinn;hkltrinn;varighet;fagsk_poeng;fagskoleutd;studp;studretn;kurstrin;komp;uhgruppe;gradmerk;hist;i97isced;i97destn;i97orien;i97varig;i97grads;i2011_P;i2011_A;i2013_F", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Nus2000", ConversionUtils.toLong(record.tokens.get(0)));
            return GenericKey.create(NuskatKey.class, values);
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
