package no.ssb.dc.collection.api.worker;

import org.junit.jupiter.api.Test;

import static no.ssb.dc.collection.api.worker.CsvSpecification.column;
import static no.ssb.dc.collection.api.worker.CsvSpecification.function;

public class CsvSpecificationTest {

    @Test
    void name() {
        CsvSpecification.ColumnKeys columnKeys = new CsvSpecification.ColumnKeys.Builder()
                .key(column().name("foo").type(String.class).position())
                .key(function().name("bar").generator(KeyGenerator.SEQUENCE).position().groupBy())
                .build();

        System.out.printf("%s%n", JsonParser.createYamlParser().toPrettyJSON(columnKeys));

        System.out.printf("%s%n", columnKeys.keys());
        System.out.printf("%s%n", columnKeys.positionKeys());
        System.out.printf("%s%n", columnKeys.groupByKeys());
    }
}
