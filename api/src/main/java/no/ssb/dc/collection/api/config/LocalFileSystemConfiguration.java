package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.MapBuilder;

import java.util.Map;

@Name("target-filesystem")
@Namespace("target")
@EnvironmentPrefix("BONG_")
@RequiredKeys({
        "target.rawdata.topic",
        "target.local-temp-folder",
        "target.filesystem.storage-folder"
})
public interface LocalFileSystemConfiguration extends TargetConfiguration {

    @Property("filesystem.storage-folder")
    String localStorageFolder();

    @Property("listing.min-interval-seconds")
    Integer listingMinIntervalSeconds();

    @Override
    default Map<String, String> defaultValues() {
        return MapBuilder.create()
                .defaults(TargetConfiguration.targetDefaultValues())
                .values("rawdata.client.provider", "filesystem")
                .values("listing.min-interval-seconds", "0")
                .specialized("avro-file.max.seconds", "60")
                .specialized("avro-file.sync.interval", Long.toString(200))
                .build();
    }

    static LocalFileSystemConfiguration create() {
        return ConfigurationFactory.createOrGet(LocalFileSystemConfiguration.class);
    }

    static LocalFileSystemConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(LocalFileSystemConfiguration.class, overrideValues);
    }

}
