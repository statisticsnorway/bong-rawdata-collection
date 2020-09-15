package no.ssb.dc.bong.ng.repository;

import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.source.CsvWorker;
import no.ssb.dc.bong.commons.source.PostgresCsvRepository;
import no.ssb.dc.bong.commons.utils.ConversionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class NGPostgresBongWorker implements CsvWorker<NGBongKey> {

    private final PostgresCsvRepository<NGBongKey> csvRepository;

    public NGPostgresBongWorker(SourcePostgresConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new PostgresCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                NGBongKey.class,
                "\\|",
                StandardCharsets.ISO_8859_1,
                NGPostgresBongWorker.csvHeader(),
                NGBongKey::isPartOfBong
        );
    }

    static String csvHeader() {
        return "#AVSBUTIKKEANLOK_NR|KJOPS_DT_TD|BONGNUMMER|KASSEPOS_NR|KASSETYPE|MOMS_BEL|TOTALRABATT_BEL" +
                "|VAREKJOP_UTEN_PANT_BEL|VAREKJOP_MED_PANT_BEL|VARELINJER_ANT|BRUTTOVARELINJE_BEL|VARERABATT_BEL|VAREANTVEKT|VAREEAN_NR|VAREENHETKODE_NR|VARENAVN|MVASATS";
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            long loc = ConversionUtils.toLong(record.tokens.get(0));
            int bong = ConversionUtils.toInteger(record.tokens.get(2));
            Date ts = ConversionUtils.toDate(record.tokens.get(1), "MM/dd/yyyy HH:mm:ss");
            return new NGBongKey(record.filename, loc, bong, ts.getTime());
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
