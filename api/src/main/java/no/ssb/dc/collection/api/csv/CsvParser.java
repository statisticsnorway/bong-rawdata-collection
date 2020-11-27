package no.ssb.dc.collection.api.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(CsvParser.class);

    static final char[] ILLEGAL_AVRO_CHARS;

    private final BufferedReader csvReader;
    private final String filepath;
    private final List<String> files;
    private final String filename;
    private final char delimiter;
    private final int dryRun;

    static {
        List<Character> illegalChars = new ArrayList<>();
        IntStream.range(33, 47).forEach(i -> illegalChars.add((char) i));
        IntStream.range(58, 64).forEach(i -> illegalChars.add((char) i));
        IntStream.range(91, 96).forEach(i -> illegalChars.add((char) i));
        IntStream.range(123, 126).forEach(i -> illegalChars.add((char) i));
        ILLEGAL_AVRO_CHARS = new char[illegalChars.size()];
        for (int i = 0; i < illegalChars.size(); i++) ILLEGAL_AVRO_CHARS[i] = illegalChars.get(i);
    }

    public CsvParser(BufferedReader csvReader, String filepath, List<String> files, String filename, char delimiter, int dryRun) {
        this.csvReader = csvReader;
        this.filepath = filepath;
        this.files = files;
        this.filename = filename;
        this.delimiter = delimiter;
        this.dryRun = dryRun;
    }

    public void parse(Consumer<Record> recordVisitor) {
        try (csvReader) {
            Map<String, Map.Entry<Integer, String>> headerMap = Collections.emptyMap(); // columnName, entry(index, avroColumnName)
            csvReader.mark(1);
            String header = csvReader.readLine();
            boolean hasTrailingDelimiter = header.endsWith(Character.toString(delimiter));
            LOG.warn("Trailing delimiter detected: {}", hasTrailingDelimiter);
            csvReader.reset();
            try (CSVParser parser = CSVFormat.RFC4180
                    .withFirstRecordAsHeader()
                    .withDelimiter(delimiter)
                    .withTrailingDelimiter(hasTrailingDelimiter)
                    .parse(csvReader)) {


                long iterationCount = 0;
                for (Iterator<CSVRecord> it = parser.iterator(); it.hasNext(); ) {
                    CSVRecord csvRecord = it.next();
                    if (csvRecord.getRecordNumber() == 1) {
                        headerMap = createHeaderMapWithAvroColumnName(csvRecord.getParser().getHeaderMap());
                    }

                    List<String> tokens = new ArrayList<>();
                    csvRecord.iterator().forEachRemaining(token -> {
                        if (token == null) {
                            tokens.add(token);
                        } else {
                            tokens.add(token.trim());
                        }
                    });

                    Record record = new Record(filepath, files, filename, headerMap, tokens, delimiter, it.hasNext());

                    if (record.isEmpty()) {
                        continue;
                    }

                    recordVisitor.accept(record);

                    if (dryRun > -1 && iterationCount < (long) dryRun) {
                        break;
                    }
                    iterationCount++;
                }
            }
        } catch (
                IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Map.Entry<Integer, String>> createHeaderMapWithAvroColumnName(Map<String, Integer> headerMap) {
        Map<String, Map.Entry<Integer, String>> headers = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            Map.Entry<Integer, String> avroHeader = Map.entry(entry.getValue(), formatToken(entry.getKey()));
            headers.put(entry.getKey(), avroHeader);
        }
        return headers;
    }

    static public String formatToken(String str) {
        return CaseUtils.toCamelCase(removeChars(str, "\\?"), true, ILLEGAL_AVRO_CHARS); // avro mappings
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

    public static class Record {
        public final String filepath;
        public final List<String> files;
        public final String filename;
        public final Map<String, Map.Entry<Integer, String>> headers;
        public final List<String> tokens;
        public final char delimiter;
        public final boolean hasNext;

        public Record(String filepath, List<String> files, String filename, Map<String, Map.Entry<Integer, String>> headers, List<String> tokens, char delimiter, boolean hasNext) {
            this.filepath = filepath;
            this.files = files;
            this.filename = filename;
            this.headers = headers;
            this.tokens = tokens;
            this.delimiter = delimiter;
            this.hasNext = hasNext;
        }

        public boolean isEmpty() {
            return Arrays.stream(tokens.toArray(new String[0])).allMatch(Record::isEmpty);
        }

        static boolean isEmpty(String token) {
            return token == null || "".equals(token);
        }

        public String asHeader() {
            return headers.values().stream().map(Map.Entry::getValue).collect(Collectors.joining(Character.toString(delimiter)));
        }

        public String asLine() {
            return String.join(Character.toString(delimiter), tokens);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Record record = (Record) o;
            return delimiter == record.delimiter &&
                    Objects.equals(filepath, record.filepath) &&
                    Objects.equals(filename, record.filename) &&
                    Objects.equals(headers, record.headers) &&
                    Objects.equals(tokens, record.tokens);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filepath, filename, headers, tokens, delimiter);
        }
    }

    public static class ColumnDefinition {
        public final int index;
        public final String name;
        public final String avroName;

        public ColumnDefinition(int index, String name, String avroName) {
            this.index = index;
            this.name = name;
            this.avroName = avroName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnDefinition that = (ColumnDefinition) o;
            return index == that.index &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(avroName, that.avroName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, name, avroName);
        }

        @Override
        public String toString() {
            return "ColumnDefinition{" +
                    "index=" + index +
                    ", name='" + name + '\'' +
                    ", avroName='" + avroName + '\'' +
                    '}';
        }
    }
}
