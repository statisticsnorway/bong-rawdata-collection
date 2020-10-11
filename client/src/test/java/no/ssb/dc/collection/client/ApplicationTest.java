package no.ssb.dc.collection.client;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class ApplicationTest {

    @Test
    void printCommands() {
        Application app = Application.create(Map.of(
                "action", "help",
                "target", ""
        ));
        app.printCommands();
    }
}
