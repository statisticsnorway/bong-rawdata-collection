package no.ssb.dc.bong.coop;

import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.source.CsvWorker;
import no.ssb.dc.bong.commons.source.PostgresCsvRepository;
import no.ssb.dc.bong.commons.utils.ConversionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class CoopPostgresBongWorker implements CsvWorker<CoopBongKey> {

    private final PostgresCsvRepository<CoopBongKey> csvRepository;

    public CoopPostgresBongWorker(SourcePostgresConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new PostgresCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                CoopBongKey.class,
                "\\|",
                StandardCharsets.UTF_8,
                CoopPostgresBongWorker.csvHeader(),
                CoopBongKey::isPartOfBong
        );
    }

    static String csvHeader() {
        return "Dato;Butikk_nr;Bongnr;GTIN;Coop_varnenr;Vare;Dato_tid;Mengde;Enhet;Pris;Valuta";
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            long loc = ConversionUtils.toLong(record.tokens.get(1));
            int bong = ConversionUtils.toInteger(record.tokens.get(2));
            Date ts = ConversionUtils.toDate(record.tokens.get(6), "yyyyMMddHHmmss"); // e.g. 20181001101119
            return new CoopBongKey(record.filename, loc, bong, ts.getTime());
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
