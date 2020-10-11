package no.ssb.dc.collection.api.config;

import java.util.Map;

@Name("source-no-db")
@Prefix("source.")
@RequiredKeys({
        "source.rawdata.topic",
        "source.csv.filepath",
        "source.csv.files"
})
public interface SourceNoDbConfiguration extends SourceConfiguration {

    @Override
    default Map<String, String> defaultValues() {
        return Map.of(
                "queue.poolSize", "25000", // flush buffer on threshold
                "queue.keyBufferSize", "511",
                "queue.valueBufferSize", "2048"
        );
    }

    static SourceNoDbConfiguration create() {
        return ConfigurationFactory.createOrGet(SourceNoDbConfiguration.class);
    }

    static SourceNoDbConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourceNoDbConfiguration.class, overrideValues);
    }
}
