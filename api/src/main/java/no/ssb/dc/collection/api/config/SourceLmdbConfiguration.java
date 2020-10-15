package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.MapBuilder;

import java.util.Map;

@Name("source-lmdb")
@Namespace("source")
@EnvironmentPrefix("BONG_")
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
        return MapBuilder.create()
                .defaults(SourceConfiguration.sourceDefaultValues())
                .values("lmdb.sizeInMb", "500")
                .build();
    }

    static SourceLmdbConfiguration create() {
        return ConfigurationFactory.createOrGet(SourceLmdbConfiguration.class);
    }

    static SourceLmdbConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourceLmdbConfiguration.class, overrideValues);
    }

}
