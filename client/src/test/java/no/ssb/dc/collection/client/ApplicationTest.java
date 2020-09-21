package no.ssb.dc.collection.client;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import org.junit.jupiter.api.Test;

public class ApplicationTest {

    @Test
    void printCommands() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("action", "help")
                .values("target", "")
                .build();

        Application app = Application.create(configuration);
        app.printCommands();
    }
}
