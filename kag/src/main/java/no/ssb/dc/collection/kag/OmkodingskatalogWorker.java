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

public class OmkodingskatalogWorker implements CsvWorker<OmkodingskatalogKey> {

    private final LmdbCsvRepository<OmkodingskatalogKey> csvRepository;

    public OmkodingskatalogWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                OmkodingskatalogKey.class,
                "\\;",
                StandardCharsets.ISO_8859_1,
                OmkodingskatalogWorker.csvHeader(),
                OmkodingskatalogKey::equals
        );
    }

    // grsk_skoler_2019.csv
    private static String csvHeader() {
        return CamelCaseHelper.formatCsvHeader("fskolenr;skolekom;skolenavn;orgnr;orgnrbed;naering;eierf;feil_orgnr;antall_elever;kobl_kilde", ";");
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("filename", record.filename);
            values.put("Fskolenr", ConversionUtils.toLong(record.tokens.get(0)));
            values.put("Skolekom", ConversionUtils.toLong(record.tokens.get(1)));
            return GenericKey.create(OmkodingskatalogKey.class, values);
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
