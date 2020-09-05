package no.ssb.dc.bong.commons.config;

import no.ssb.config.DynamicConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class SourcePostgresConfiguration extends AbstractConfiguration {

    public SourcePostgresConfiguration() {
        this(new LinkedHashMap<>());
    }

    public SourcePostgresConfiguration(Map<String, String> overrideKeyValuePairs) {
        super("source.",
                Map.of(
                        "postgres.driver.host", "localhost",
                        "postgres.driver.port", "5432",
                        "postgres.driver.user", "bong",
                        "postgres.driver.password", "bong",
                        "postgres.driver.database", "bong",
                        "queue.poolSize", "25000", // flush buffer on threshold
                        "queue.keyBufferSize", "511",
                        "queue.valueBufferSize", "2048",
                        "csv.dateFormat", "MM/dd/yyyy HH:mm:ss"
                ),
                overrideKeyValuePairs
        );
    }

    public SourcePostgresConfiguration(Map<String, String> defaultKeyValuePairs, Map<String, String> overrideKeyValuePairs) {
        super("source.",
                defaultKeyValuePairs,
                overrideKeyValuePairs
        );
    }

    @Override
    public String name() {
        return "psql";
    }

    @Override
    public Set<String> requiredKeys() {
        return Set.of(
                "source.postgres.driver.host",
                "source.postgres.driver.port",
                "source.postgres.driver.user",
                "source.postgres.driver.password",
                "source.postgres.driver.database"
        );
    }

    @Override
    public DynamicConfiguration asDynamicConfiguration() {
        return configuration;
    }

}
