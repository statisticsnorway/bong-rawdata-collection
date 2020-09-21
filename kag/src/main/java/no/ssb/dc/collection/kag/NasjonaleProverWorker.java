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

public class NasjonaleProverWorker implements CsvWorker<NasjonaleProverKey> {

    private final LmdbCsvRepository<NasjonaleProverKey> csvRepository;

    public NasjonaleProverWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                NasjonaleProverKey.class,
                "\\;",
                StandardCharsets.US_ASCII,
                NasjonaleProverWorker.csvHeader(),
                NasjonaleProverKey::equals
        );
    }

    // NUDB_NP.csv
    static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("SNR_NUDB;AARGANG;ORGNR;ORGNRBED;SKOLEKOM;SKOLEFYLKE;SKOLETYPE;AVGIVERSKOLE_ORGNR;AVGIVERSKOLE_ORGNRBED;DELTATTSTATUS;PROVE;MESTRINGSNIVAA;POENG;OPPGAVESETT;ANDELPOENG;GRUPPENUMMER;SKALAPOENG", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("SnrNudb", ConversionUtils.toString(record.tokens.get(0)));
            return GenericKey.create(NasjonaleProverKey.class, values);
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
