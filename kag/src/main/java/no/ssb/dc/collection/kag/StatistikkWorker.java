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

public class StatistikkWorker implements CsvWorker<StatistikkKey> {

    private final LmdbCsvRepository<StatistikkKey> csvRepository;

    public StatistikkWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                StatistikkKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                StatistikkWorker.csvHeader(),
                StatistikkKey::isPartOf
        );
    }

    // STATFIL_KAR_RES_G2019G2020.csv
    static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("""
                fnr;snr_nudb;skolear;fskolenr;skolenavn;orgnr;orgnrbed;FV_snavn;skolekom;eierf;nus2000_far;SOSBAK;nus2000_mor;nus2000;\
                KJOENN;statborg;KOMMNR;fodeland;landbak;invkat;alder;grunnskolepoeng;studretn;kurstrin;ktrinn;kltrinn;naering;utd;kilde;\
                komp;FV_elevstatus;koblet;trinnst;med_pub3;med_pub2;insttype;med_pub_kar;med_pub_res;stpENG0012;stpENG0013;stpENG0029;stpFSP0042;\
                stpFSP0132;stpFSP0162;stpKHV0010;stpKRO0020;stpMAT0010;stpMHE0010;stpMUS0010;stpNAT0010;stpNOR0068;stpNOR0214;stpNOR0215;stpNOR0216;\
                stpRLE0030;stpSAF0010;skrENG0012;munENG0013;munFSP0132;skrMAT0010;munMAT0011;munNAT0010;skrNOR0214;munNOR0216;munSAF0010;mnivaa_eng05;\
                mnivaa_les05;mnivaa_reg05;mnivaa_eng08;mnivaa_les08;mnivaa_reg0;
                """, ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Fnr", ConversionUtils.toString(record.tokens.get(0)));
            return GenericKey.create(StatistikkKey.class, values);
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
