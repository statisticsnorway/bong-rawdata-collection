package no.ssb.dc.collection.api.source;

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Pattern;

public class DetectDataType {

    static final List<String> booleanValues = List.of("true", "false", "sann", "usann");
    static final Pattern longPattern = Pattern.compile("[+-]?[0-9]+");
    static final Pattern longHexPattern = Pattern.compile("(?:0x[1-9A-Fa-f][0-9A-Fa-f]*)");
    static final Pattern doublePattern = Pattern.compile("^[-+]?[0-9]*[.,]?[0-9]+([eE][-+]?[0-9]+)?$");

    public static DataType detect(String value) {
        if (value == null || "".equals(value)) {
            return DataType.STRING;
        }

        try {
            if (booleanValues.stream().anyMatch(bool -> bool.equalsIgnoreCase(value))) {
                return DataType.BOOLEAN;
            }

            if (longPattern.matcher(value).matches()) {
                return isInteger(Long.parseLong(value)) ? DataType.INTEGER : DataType.LONG;
            }

            if (doublePattern.matcher(value).matches()) {
                String doubleOrFloatValueString = value.replace(",", ".");
                Double doubleValue = Double.parseDouble(doubleOrFloatValueString);
                return isFloat(doubleOrFloatValueString, doubleValue) ? DataType.FLOAT : DataType.DOUBLE;
            }

        } catch (Exception e) {
            return DataType.STRING;
        }

        return DataType.STRING;
    }

    static boolean isInteger(long value) {
        return !(value < (long) Integer.MIN_VALUE || value > (long) Integer.MAX_VALUE);
    }

    static boolean isFloat(String doubleOrFloatValueAsString, Double value) {
        BigDecimal doubleBD = new BigDecimal(""+value);
        BigDecimal floatBD = new BigDecimal(""+Float.parseFloat(doubleOrFloatValueAsString));
        return doubleBD.equals(floatBD);
    }

}
