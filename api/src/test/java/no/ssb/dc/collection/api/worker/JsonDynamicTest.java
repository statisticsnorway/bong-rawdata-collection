package no.ssb.dc.collection.api.worker;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import static no.ssb.dc.collection.api.worker.CsvSpecification.*;

public class JsonDynamicTest {

    static final CsvSpecification specification = new CsvSpecification.Builder()
            .backend(BackendProvider.NO_CACHE)
            .metadata(metadata()
                    .source("source")
                    .dataset("dataset")
                    .tag("tag")
                    .description("bla bla")
            )
            .fileDescriptor(fileDescriptor()
                    .delimiter(';')
                    .charset(StandardCharsets.US_ASCII)
                    .contentType("text/csv")
                    .files("generic.csv")
            )
            .columnKeys(column()
                    .name("a")
                    .type(String.class)
                    .position()
                    .groupBy()
            )
            .columnKeys(column()
                    .name("b")
                    .type(String.class)
                    .groupBy()
            )
            .columnKeys(column()
                    .name("c")
                    .type(Date.class)
                    .format("MM/dd/yyyy HH:mm:ss")
                    .groupBy()
            )
            .columnKeys(function()
                    .name("seq")
                    .generator(KeyGenerator.SEQUENCE)
                    .position()
            )
            .build();

    @Test
    void spec() {

    }
}
