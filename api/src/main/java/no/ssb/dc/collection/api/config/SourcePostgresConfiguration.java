package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.MapBuilder;

import java.util.Map;

@Name("source-postgres")
@Namespace("source")
@EnvironmentPrefix("BONG_")
@RequiredKeys({
        "source.postgres.driver.host",
        "source.postgres.driver.port",
        "source.postgres.driver.user",
        "source.postgres.driver.password",
        "source.postgres.driver.database",
        "source.rawdata.topic",
        "source.csv.filepath",
        "source.csv.files"
})
public interface SourcePostgresConfiguration extends SourceConfiguration {

    @Property("postgres.driver.host")
    String postgresDriverHost();

    @Property("postgres.driver.port")
    String postgresDriverPort();

    @Property("postgres.driver.user")
    String postgresDriverUser();

    @Property("postgres.driver.password")
    String postgresDriverPassword();

    @Property("postgres.driver.database")
    String postgresDriverDatabase();

    @Override
    default Map<String, String> defaultValues() {
        return MapBuilder.create()
                .defaults(SourceConfiguration.sourceDefaultValues())
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "bong")
                .values("postgres.driver.password", "bong")
                .values("postgres.driver.database", "bong")
                .build();
    }

    static SourcePostgresConfiguration create() {
        return ConfigurationFactory.createOrGet(SourcePostgresConfiguration.class);
    }

    static SourcePostgresConfiguration create(Map<String, String> overrideValues) {
        return ConfigurationFactory.createOrGet(SourcePostgresConfiguration.class, overrideValues);
    }
}
