package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Map;

@Name("source-postgres")
@Prefix("source.")
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
        return Map.of(
                "queue.poolSize", "25000", // flush buffer on threshold
                "queue.keyBufferSize", "511",
                "queue.valueBufferSize", "2048",
                "postgres.driver.host", "localhost",
                "postgres.driver.port", "5432",
                "postgres.driver.user", "bong",
                "postgres.driver.password", "bong",
                "postgres.driver.database", "bong"
        );
    }

    static SourcePostgresConfiguration create() {
        return new DynamicProxy<>(SourcePostgresConfiguration.class).instance();
    }

    static SourcePostgresConfiguration create(Map<String, String> overrideValues) {
        return new DynamicProxy<>(SourcePostgresConfiguration.class, overrideValues).instance();
    }
}
