package no.ssb.dc.collection.kag;

import org.apache.commons.text.CaseUtils;

import java.util.ArrayList;
import java.util.List;

public class CamelCaseHelper {

    static public String formatCsvHeader(String csvHeader, String delimeter) {
        String[] tokens = csvHeader.split(delimeter);
        for (int n = 0; n < tokens.length; n++) {
            tokens[n] = CaseUtils.toCamelCase(removeChars(tokens[n], "\\?"), true, '_', '(', ')');
        }
        return removeLastDelimeterIfExists(String.join(delimeter, List.of(tokens)), delimeter);
    }

    static public String formatToken(String str, String delimeter) {
        return CaseUtils.toCamelCase(removeLastDelimeterIfExists(removeChars(str, "\\?"), delimeter), true, '_', '(', ')');
    }

    static public String removeChars(String str, String... chars) {
        List<String> removeChars = new ArrayList<>(List.of(chars));
        removeChars.add("\t");
        removeChars.add("\r");
        removeChars.add("\n");
        for (String ch : removeChars) {
            str = str.replaceAll(ch, "");
        }
        return str;
    }

    static public String removeLastDelimeterIfExists(String str, String delimeter) {
        if (str.endsWith(delimeter)) {
            return str.substring(0, str.length() - 1);
        }
        return str;
    }
}
