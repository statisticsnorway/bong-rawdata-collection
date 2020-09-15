package no.ssb.dc.bong.ng.repository;

import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.source.CsvWorker;
import no.ssb.dc.bong.commons.source.LmdbCsvRepository;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class NGLmdbArticleWorker implements CsvWorker<NGArticleKey> {

    private final LmdbCsvRepository<NGArticleKey> csvRepository;

    public NGLmdbArticleWorker(SourceLmdbConfiguration sourceConfiguration, TargetConfiguration targetConfiguration, Class<NGArticleKey> keyClass, String delimeter, Charset csvCharset) {
        csvRepository = new LmdbCsvRepository<>(
                sourceConfiguration,
                targetConfiguration,
                NGArticleKey.class,
                "\\|",
                StandardCharsets.ISO_8859_1,
                NGLmdbArticleWorker.csvHeader(),
                NGArticleKey::equals
        );
    }

    static String csvHeader() {
        return "#VAREEAN_NR|GYLDIGFRA_DT|GYLDIGTIL_DT|VARENAVN|VARENAVNPAKNING|ENVAGRUPPE_NR|ENVAGRUPPE_NV|ALLERGITESTET_JN|"+
                "BLAA_ENGEL_JN|BRA_MILJOVAL_JN|GENMODIFISERT_JN|HALAL_JN|LAVOMLOPSHAST_JN|MILJOKODESVANEN_JN|MILJOKODEBLOMSTEN_JN|"+
                "MILJOKODEDEBIO_JN|MILJOKODEENERGYSTAR_JN|MILJOKODEEUOKOLOGI_JN|MILJOKODEFAIRTRADE_JN|MILJOKODEFSC_JN|MILJOKODEKRAV_JN|"
                +"MILJOKODEMSC_JN|MILJOKODENOKKELHULLET_JN|MILJOKODENYTNORGE_JN|MILJOKODETCO_JN|OKOLOGISK_JN|UJEVNVEKT_JN";
    }

    @Override
    public void prepare() {
        csvRepository.prepare(record -> null);
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
