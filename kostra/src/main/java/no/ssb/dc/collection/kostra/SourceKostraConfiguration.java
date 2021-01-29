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

    /*
     * Refer to shell scripts: rawdata-collection.sh (set env-vars using cli options) passed to docker-collection-cli.sh (read env-var mappings)
     */

    // /dapla/bin/conf/application-kostra.properties#source.path="/source" (which is a volume mount)
    @Property("path")
    String sourcePath();

    // /dapla/bin/conf/application-kostra.properties#source.file="$JSON_FILE" (env-var substitute)
    @Property("file")
    String sourceFile();

    // /dapla/bin/conf/application-kostra.properties#source.specification.file="$SPECIFICATION_FILE" (env-var substitute)
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
