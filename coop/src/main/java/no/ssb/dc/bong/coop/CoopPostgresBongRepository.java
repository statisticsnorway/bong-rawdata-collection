package no.ssb.dc.bong.coop;

import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.postgres.PostgresCsvRepository;
import no.ssb.dc.bong.commons.rawdata.ConversionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.function.Consumer;

public class CoopPostgresBongRepository extends PostgresCsvRepository<CoopBongKey> {

    static final Logger LOG = LoggerFactory.getLogger(CoopPostgresBongRepository.class);

    public CoopPostgresBongRepository(SourcePostgresConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        super(sourceConfiguration, targetConfiguration, CoopBongKey.class, "\\;", StandardCharsets.ISO_8859_1);
    }

    @Override
    public String csvHeader() {
        return "Dato;Butikk_nr;Bongnr;GTIN;Coop_varnenr;Vare;Dato_tid;Mengde;Enhet;Pris;Valuta";
    }

    @Override
    public void prepare() {
        super.prepare(record -> {
            long loc = ConversionUtils.toLong(record.tokens.get(1));
            int bong = ConversionUtils.toInteger(record.tokens.get(2));
            Date ts = ConversionUtils.toDate(record.tokens.get(6), "yyyyMMddHHmmss"); // e.g. 20181001101119
            return new CoopBongKey(record.filename, loc, bong, ts.getTime());
        });
    }

    @Override
    public void consume(Consumer<Map<CoopBongKey, String>> entrySetCallback) {
        super.consume(entrySetCallback, CoopBongKey::isPartOfBong);
    }

    @Override
    public void produce() {
        super.produce(CoopBongKey::isPartOfBong);
    }
}
