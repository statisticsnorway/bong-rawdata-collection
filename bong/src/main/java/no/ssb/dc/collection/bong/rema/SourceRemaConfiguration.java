package no.ssb.dc.collection.bong.rema;

import no.ssb.dc.collection.api.config.BaseConfiguration;
import no.ssb.dc.collection.api.config.Name;
import no.ssb.dc.collection.api.config.Prefix;
import no.ssb.dc.collection.api.config.Property;
import no.ssb.dc.collection.api.config.RequiredKeys;

import java.util.Map;

@Name("source-rema")
@Prefix("source.")
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
        return BaseConfiguration.create(SourceRemaConfiguration.class);
    }

    static SourceRemaConfiguration create(Map<String, String> overrideValues) {
        return BaseConfiguration.create(SourceRemaConfiguration.class, overrideValues);
    }
}
