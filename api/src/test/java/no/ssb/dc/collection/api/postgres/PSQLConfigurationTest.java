package no.ssb.dc.collection.api.postgres;

import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PSQLConfigurationTest {

    @Test
    public void psqlConfiguration() {
        SourcePostgresConfiguration psqlConfiguration = SourcePostgresConfiguration.create(Map.of(
                "source.csv.files", "",
                "source.rawdata.topic", "",
                "source.csv.filepath", ""
        ));
        assertEquals("localhost", psqlConfiguration.postgresDriverHost());
        assertEquals("5432", psqlConfiguration.postgresDriverPort());
        assertEquals("bong", psqlConfiguration.postgresDriverUser());
        assertEquals("bong", psqlConfiguration.postgresDriverPassword());
        assertEquals("bong", psqlConfiguration.postgresDriverDatabase());
    }
}
