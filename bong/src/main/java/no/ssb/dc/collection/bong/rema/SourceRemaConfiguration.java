package no.ssb.dc.collection.bong.rema;

import no.ssb.dapla.migration.rawdata.onprem.config.BaseConfiguration;
import no.ssb.dapla.migration.rawdata.onprem.config.ConfigurationFactory;
import no.ssb.dapla.migration.rawdata.onprem.config.EnvironmentPrefix;
import no.ssb.dapla.migration.rawdata.onprem.config.Name;
import no.ssb.dapla.migration.rawdata.onprem.config.Namespace;
import no.ssb.dapla.migration.rawdata.onprem.config.Property;
import no.ssb.dapla.migration.rawdata.onprem.config.RequiredKeys;

import java.util.Map;

@Name("source-rema")
@Namespace("source")
@EnvironmentPrefix("BONG_")
@RequiredKeys({
        "source.year",
        "source.month"
})
public interface SourceRemaConfiguration extends BaseConfiguration {

    @Property("year")
    String year();

    @Property("month")
    String month();

    @Property("root.path")
    String rootPath();

    @Property("queue.capacity")
    Boolean hasQueueCapacity();

    @Property("queue.capacity")
    Integer queueCapacity();

    @Override
    default Map<String, String> defaultValues() {
        return Map.of(
                "root.path", "/source",
                "queue.capacity", "1000" // flush buffer on threshold
        );
    }

    static SourceRemaConfiguration create() {
        return ConfigurationFactory.createOrGet(SourceRemaConfiguration.class);
    }

    static SourceRemaConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourceRemaConfiguration.class, overrideValues);
    }
}
