package no.ssb.dc.collection.api.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class ConversionUtils {

    public static String toString(String value) {
        return value;
    }

    public static Integer toInteger(String value) {
        return Integer.valueOf(value);
    }

    public static Long toLong(String value) {
        return Long.valueOf(value);
    }

    public static Date toDate(String value, String dateFormat) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
        ZoneId zone = ZoneId.of("Europe/Oslo");
        ZoneOffset zoneOffSet = zone.getRules().getOffset(dateTime);
        return Date.from(dateTime.toInstant(zoneOffSet));
    }
}
