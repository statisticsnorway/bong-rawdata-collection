package no.ssb.dc.collection.client;

import no.ssb.dc.collection.api.config.BootstrapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ApplicationTest {

    @Test
    void printCommands() {
        BootstrapConfiguration configuration = BootstrapConfiguration.create(Map.of(
                "action", "help",
                "target", ""
        ));
        Application app = Application.create(configuration);
        app.printCommands();
    }
}
