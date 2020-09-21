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

public class ResultatWorker implements CsvWorker<ResultatKey> {

    private final LmdbCsvRepository<ResultatKey> csvRepository;

    public ResultatWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                ResultatKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                ResultatWorker.csvHeader(),
                ResultatKey::isPartOGroup
        );
    }

    // FilID6189_Data.csv
    static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader(
                """
                        FilID;RadID;RadNr;Fødselsnummer;Skoleår;Skolenummer;Programområdekode;Elevnavn;Skolenavn;Organisasjonsnummer;Startdato;Avbruddsdato;Fullførtkode;\
                        Rettstype ved inntak;Elevstatus;Godkjent realkompetansevurdering dato;Karakterpoeng;Kursprosent;Antall dager fravær;Antall enkelttimer fravær;\
                        Antall fag på elevkurset;Antall stryk på elevkurset;Bevistype;Forrige elevstatus;Målform (norsk hovedmål);Rettstype i hjemfylket;Påbyggingsrett;         
                        """
                , ";");
//        return "FilID;RadID;RadNr;Fødselsnummer;Skoleår;Skolenummer;Programområdekode;Fagkode;Fagstatus;Karakter halvårsvurdering 1;" +
//                "Karakter halvårsvurdering 2;Karakter standpunkt;Karakter skriftlig eksamen;Karakter muntlig eksamen;Karakter annen;" +
//                "Skoleår 2;Skolenummer 2;Er linja aktiv?;Elevtimer;Forrige fagstatus;Fagmerknad kode;Karakterstatus;";
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Filid", ConversionUtils.toLong(record.tokens.get(0)));
            values.put("Fnr", ConversionUtils.toLong(record.tokens.get(3)));
            values.put("Radnr", ConversionUtils.toLong(record.tokens.get(1)));
            return GenericKey.create(ResultatKey.class, values);
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
