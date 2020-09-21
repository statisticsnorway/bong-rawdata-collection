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

public class SkolekatalogWorker implements CsvWorker<SkolekatalogKey> {

    private final LmdbCsvRepository<SkolekatalogKey> csvRepository;

    public SkolekatalogWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                SkolekatalogKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                SkolekatalogWorker.csvHeader(),
                SkolekatalogKey::equals
        );
    }

    // g201910_grunnskoler.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("orgnr;orgnrforetak;orgnrbed;nace1_sn07;nace2_sn07;nace3_sn07;skole1;skole2;skole3;eierf;rectype;bnr;eiernavn;navn_inst;skolekom;reg_type;mdelr;delreg_merke;org_form;gml_org_form;status;skoletype;orgnr_eier", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Orgnr", ConversionUtils.toString(record.tokens.get(0)));
            return GenericKey.create(SkolekatalogKey.class, values);
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
