package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.MapBuilder;

import java.util.Map;

@Name("target-gcs")
@Namespace("target")
@EnvironmentPrefix("BONG_")
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
        return MapBuilder.create()
                .defaults(TargetConfiguration.targetDefaultValues())
                .values("rawdata.client.provider", "gcs")
                .values("gcs.credential-provider", "service-account")
                .values("gcs.listing.min-interval-seconds", "30")
                .specialized("avro-file.max.seconds", "86400")
                .specialized("avro-file.sync.interval", Long.toString(524288))
                .build();
    }

    static GCSConfiguration create() {
        return ConfigurationFactory.createOrGet(GCSConfiguration.class);
    }

    static GCSConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(GCSConfiguration.class, overrideValues);
    }

}
