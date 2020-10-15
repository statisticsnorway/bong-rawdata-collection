package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.MapBuilder;

import java.util.Map;

@Name("source-no-db")
@Namespace("source")
@EnvironmentPrefix("BONG_")
@RequiredKeys({
        "source.rawdata.topic",
        "source.csv.filepath",
        "source.csv.files"
})
public interface SourceNoDbConfiguration extends SourceConfiguration {

    @Override
    default Map<String, String> defaultValues() {
        return MapBuilder.create()
                .defaults(SourceConfiguration.sourceDefaultValues())
                .build();
    }

    static SourceNoDbConfiguration create() {
        return ConfigurationFactory.createOrGet(SourceNoDbConfiguration.class);
    }

    static SourceNoDbConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourceNoDbConfiguration.class, overrideValues);
    }
}
