package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Map;

@Name("source-lmdb")
@Prefix("source.")
@RequiredKeys({
        "source.lmdb.path",
        "source.rawdata.topic",
        "source.csv.filepath",
        "source.csv.files"
})
public interface SourceLmdbConfiguration extends SourceConfiguration {

    @Property("lmdb.path")
    String lmdbPath();

    @Property("lmdb.sizeInMb")
    Boolean hasLmdbSizeInMb();

    @Property("lmdb.sizeInMb")
    Integer lmdbSizeInMb();

    @Override
    default Map<String, String> defaultValues() {
        return Map.of(
                "queue.poolSize", "25000", // flush buffer on threshold
                "queue.keyBufferSize", "511",
                "queue.valueBufferSize", "2048",
                "lmdb.sizeInMb", "500"
        );
    }

    static SourceLmdbConfiguration create() {
        return new DynamicProxy<>(SourceLmdbConfiguration.class).instance();
    }

    static SourceLmdbConfiguration create(Map<String, String> overrideValues) {
        return new DynamicProxy<>(SourceLmdbConfiguration.class, overrideValues).instance();
    }

}
