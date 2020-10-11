package no.ssb.dc.collection.api.config;

import java.util.Map;

@Name("target-filesystem")
@Prefix("target.")
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
        return Map.of(
                "rawdata.client.provider", "filesystem",
                "avro-file.max.seconds", "60",
                "avro-file.max.bytes", Long.toString(64 * 1024 * 1024), // 64 MiB
                "avro-file.sync.interval", Long.toString(200),
                "listing.min-interval-seconds", "0"
        );
    }

    static LocalFileSystemConfiguration create() {
        return ConfigurationFactory.createOrGet(LocalFileSystemConfiguration.class);
    }

    static LocalFileSystemConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(LocalFileSystemConfiguration.class, overrideValues);
    }

}
