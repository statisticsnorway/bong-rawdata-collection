package no.ssb.dc.collection.api.config;

import no.ssb.config.DynamicConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LocalFileSystemConfiguration extends TargetConfiguration {

    public LocalFileSystemConfiguration() {
        this(new LinkedHashMap<>());
    }

    public LocalFileSystemConfiguration(Map<String, String> overrideKeyValuePairs) {
        super("target.",
                Map.of(
                        "rawdata.client.provider", "filesystem",
                        "avro-file.max.seconds", "60",
                        "avro-file.max.bytes", Long.toString(64 * 1024 * 1024), // 64 MiB
                        "avro-file.sync.interval", Long.toString(200),
                        "listing.min-interval-seconds", "0"
                ),
                overrideKeyValuePairs
        );
    }

    @Override
    public String name() {
        return "target-filesystem";
    }

    @Override
    public Set<String> requiredKeys() {
        return Set.of(
                "target.rawdata.topic",
                "target.local-temp-folder",
                "target.filesystem.storage-folder"
        );
    }

    @Override
    public DynamicConfiguration asDynamicConfiguration() {
        return configuration;
    }

}