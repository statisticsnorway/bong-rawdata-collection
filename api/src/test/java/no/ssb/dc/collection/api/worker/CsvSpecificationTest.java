package no.ssb.dc.collection.api.worker;

import org.junit.jupiter.api.Test;

public class CsvSpecificationTest {

    @Test
    void name() {
        CsvSpecification.Keys keys = new CsvSpecification.Keys.Builder()
                .key(new CsvSpecification.ColumnKey.Builder().name("foo").type(String.class).position())
                .key(new CsvSpecification.FunctionKey.Builder().name("bar").generator(CsvSpecification.KeyGenerator.SEQUENCE).position().groupBy())
                .build();

        System.out.printf("%s%n", JsonParser.createYamlParser().toPrettyJSON(keys));

        System.out.printf("%s%n", keys.keys());
        System.out.printf("%s%n", keys.positionKeys());
        System.out.printf("%s%n", keys.groupByKeys());
    }
}
