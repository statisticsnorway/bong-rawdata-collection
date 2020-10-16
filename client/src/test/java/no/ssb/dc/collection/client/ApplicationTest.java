package no.ssb.dc.collection.client;

import no.ssb.dc.collection.api.config.BootstrapConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApplicationTest {

    @Test
    void dynamicProxyEqualsAndToString() {
        BootstrapConfiguration bootstrapConfiguration = BootstrapConfiguration.create();
        System.out.printf("%s%n", bootstrapConfiguration.toString());
        System.out.printf("%s%n", bootstrapConfiguration.hashCode());
        assertEquals(bootstrapConfiguration, bootstrapConfiguration);
    }

    @Test
    void printCommands() {
        Application app = Application.create(Map.of(
                "action", "help",
                "target", ""
        ));
        app.printCommands();
    }

}
