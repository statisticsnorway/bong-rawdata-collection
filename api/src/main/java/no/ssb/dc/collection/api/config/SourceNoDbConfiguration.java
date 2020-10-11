package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

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
        return new DynamicProxy<>(SourceNoDbConfiguration.class).instance();
    }

    static SourceNoDbConfiguration create(Map<String, String> overrideValues) {
        return new DynamicProxy<>(SourceNoDbConfiguration.class, overrideValues).instance();
    }
}
