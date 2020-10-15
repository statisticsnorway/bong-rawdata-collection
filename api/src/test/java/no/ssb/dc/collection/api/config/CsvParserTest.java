package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.csv.CsvParser;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.List;

public class CsvParserTest {

    static final Logger LOG = LoggerFactory.getLogger(CsvParserTest.class);

    static final String CSV_1 = """
            a;b;c
            1;2;3
            2;3;4
            3;4;5
            4;5;6
            5;6;7
            6;7;8
            """;

    @Test
    public void csvParser() {
        BufferedReader reader = new BufferedReader(new StringReader(CSV_1));
        CsvParser parser = new CsvParser(reader, Paths.get(".").toString(), List.of("test"), "test", ';', -1);
        parser.parse(record -> {
            LOG.trace("record: {} -- {} -- {}", record.tokens.get(0), record.asLine(), record.headers);
        });
    }
}
