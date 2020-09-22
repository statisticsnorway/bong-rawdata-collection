package no.ssb.dc.collection.kag;

import org.junit.jupiter.api.Test;

public class TextBlockTest {

    @Test
    void printTextBlock() {
//        System.out.printf("%s%n", StatistikkWorker.csvHeader());
        System.out.printf("%s%n", ResultatWorker.csvHeader());
//        System.out.printf("%s%n", KarakterWorker.csvHeader());
    }

    @Test
    void formatCamelCase() {
        System.out.printf("%s%n", CamelCaseHelper.formatToken("Nus2000", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("SNR_NUDB", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("orgnrbed", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("FagKode", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("Snr;", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("THIS IS AN EXAMPLE STRING?;", ";"));
        System.out.printf("%s%n", CamelCaseHelper.formatToken("THIS_IS_AN_EXAMPLE _STRING?;", ";"));
    }
}
