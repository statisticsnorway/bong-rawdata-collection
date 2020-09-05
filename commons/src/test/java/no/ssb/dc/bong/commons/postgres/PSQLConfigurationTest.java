package no.ssb.dc.bong.commons.postgres;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PSQLConfigurationTest {

    @Test
    public void psqlConfiguration() {
        SourcePostgresConfiguration psqlConfiguration = new SourcePostgresConfiguration();
        DynamicConfiguration config = psqlConfiguration.asDynamicConfiguration();
        assertEquals("localhost", config.evaluateToString("postgres.driver.host"));
        assertEquals("5432", config.evaluateToString("postgres.driver.port"));
        assertEquals("bong", config.evaluateToString("postgres.driver.user"));
        assertEquals("bong", config.evaluateToString("postgres.driver.password"));
        assertEquals("bong", config.evaluateToString("postgres.driver.database"));
    }
}
