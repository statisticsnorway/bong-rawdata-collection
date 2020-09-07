package no.ssb.dc.bong.commons.config;

import no.ssb.config.DynamicConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class GCSConfiguration extends TargetConfiguration {

    public GCSConfiguration() {
        this(new LinkedHashMap<>());
    }

    public GCSConfiguration(Map<String, String> overrideKeyValuePairs) {
        super("target.",
                Map.of(
                        "rawdata.client.provider", "gcs",
                        "gcs.credential-provider", "service-account",
                        "gcs.listing.min-interval-seconds", "30",
                        "avro-file.max.seconds", "86400",
                        "avro-file.max.bytes", Long.toString(64 * 1024 * 1024), // local export should be 512 MiB
                        "avro-file.sync.interval", Long.toString(524288)
                ),
                overrideKeyValuePairs
        );
    }

    @Override
    public String name() {
        return "target-gcs";
    }

    @Override
    public Set<String> requiredKeys() {
        return Set.of(
                "target.rawdata.topic",
                "target.gcs.bucket-name",
                "target.gcs.service-account.key-file",
                "target.local-temp-folder"
        );
    }

    @Override
    public DynamicConfiguration asDynamicConfiguration() {
        return configuration;
    }

}
