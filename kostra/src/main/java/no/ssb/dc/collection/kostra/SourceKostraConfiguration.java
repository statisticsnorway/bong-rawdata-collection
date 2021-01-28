package no.ssb.dc.collection.kostra;

import no.ssb.dc.collection.api.config.BaseConfiguration;
import no.ssb.dc.collection.api.config.ConfigurationFactory;
import no.ssb.dc.collection.api.config.EnvironmentPrefix;
import no.ssb.dc.collection.api.config.Name;
import no.ssb.dc.collection.api.config.Namespace;
import no.ssb.dc.collection.api.config.Property;
import no.ssb.dc.collection.api.config.RequiredKeys;

import java.util.Map;

@Name("source-kostra")
@Namespace("source")
@EnvironmentPrefix("BONG_")
@RequiredKeys({
        "source.path"
})
public interface SourceKostraConfiguration extends BaseConfiguration {

    @Property("path")
    String sourcePath();

    @Property("file")
    String sourceFile();

    @Property("specification.file")
    String specificationFile();

    @Property("queue.capacity")
    Boolean hasQueueCapacity();

    @Property("queue.capacity")
    Integer queueCapacity();

    @Override
    default Map<String, String> defaultValues() {
        return Map.of(
                "path", "/source",
                "queue.capacity", "1000" // flush buffer on threshold
        );
    }

    static SourceKostraConfiguration create() {
        return ConfigurationFactory.createOrGet(SourceKostraConfiguration.class);
    }

    static SourceKostraConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourceKostraConfiguration.class, overrideValues);
    }
}
