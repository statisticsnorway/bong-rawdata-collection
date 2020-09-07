package no.ssb.dc.bong.ng.repository;

import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.lmdb.LmdbCsvRepository;
import no.ssb.dc.bong.commons.rawdata.ConversionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.function.Consumer;

public class NGLmdbBongRepository extends LmdbCsvRepository<NGBongKey> {

    public NGLmdbBongRepository(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        super(sourceConfiguration, targetConfiguration, NGBongKey.class, "\\|", StandardCharsets.ISO_8859_1);
    }

    @Override
    public String csvHeader() {
        return "#AVSBUTIKKEANLOK_NR|KJOPS_DT_TD|BONGNUMMER|KASSEPOS_NR|KASSETYPE|MOMS_BEL|TOTALRABATT_BEL" +
                "|VAREKJOP_UTEN_PANT_BEL|VAREKJOP_MED_PANT_BEL|VARELINJER_ANT|BRUTTOVARELINJE_BEL|VARERABATT_BEL|VAREANTVEKT|VAREEAN_NR|VAREENHETKODE_NR|VARENAVN|MVASATS";
    }

    @Override
    public void prepare() {
        super.prepare(record -> {
            long loc = ConversionUtils.toLong(record.tokens.get(0));
            int bong = ConversionUtils.toInteger(record.tokens.get(2));
            Date ts = ConversionUtils.toDate(record.tokens.get(1), "MM/dd/yyyy HH:mm:ss");
            return new NGBongKey(record.filename, loc, bong, ts.getTime());
        });
    }

    @Override
    public void consume(Consumer<Map<NGBongKey, String>> entrySetCallback) {
        super.consume(entrySetCallback, NGBongKey::isPartOfBong);
    }

    @Override
    public void produce() {
        super.produce(NGBongKey::isPartOfBong);
    }
}
