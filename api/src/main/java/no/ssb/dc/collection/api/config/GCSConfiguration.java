package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Map;

@Name("target-gcs")
@Prefix("target.")
@RequiredKeys({
        "target.rawdata.topic",
        "target.gcs.bucket-name",
        "target.gcs.service-account.key-file",
        "target.local-temp-folder"
})
public interface GCSConfiguration extends TargetConfiguration {

    @Property("gcs.bucket-name")
    String bucketName();

    @Property("gcs.service-account.key-file")
    String gcsServiceAccountKeyFile();

    @Property("gcs.credential-provider")
    String gcsCredentialProvider();

    @Property("gcs.listing.min-interval-seconds")
    Integer gcsListingMinIntervalSeconds();

    @Override
    default Map<String, String> defaultValues() {
        return Map.of(
                "rawdata.client.provider", "gcs",
                "gcs.credential-provider", "service-account",
                "gcs.listing.min-interval-seconds", "30",
                "avro-file.max.seconds", "86400",
                "avro-file.max.bytes", Long.toString(64 * 1024 * 1024), // local export should be 512 MiB
                "avro-file.sync.interval", Long.toString(524288)
        );
    }

    static GCSConfiguration create() {
        return BaseConfiguration.create(GCSConfiguration.class);
    }

    static GCSConfiguration create(Map<String, String> overrideValues) {
        return new DynamicProxy<>(GCSConfiguration.class, overrideValues).instance();
    }

}
