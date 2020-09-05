package no.ssb.dc.bong.commons.csv;

import no.ssb.config.DynamicConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class CSVReader {

    private static final Logger LOG = LoggerFactory.getLogger(CSVReader.class);

    private final int skipHeader;
    private final Path csvFilePath;
    private final List<String> csvFiles = new ArrayList<>();

    public CSVReader(DynamicConfiguration configuration, boolean skipHeader) {
        this.skipHeader = skipHeader ? 1 : 0;
        String filepath = configuration.evaluateToString("csv.filepath");
        if (filepath.contains("$PROJECT_DIR")) {
            filepath = filepath.replace("$PROJECT_DIR", Paths.get(".").normalize().resolve(filepath).toString());
        }
        this.csvFilePath = Paths.get(filepath);
        csvFiles.addAll(List.of(configuration.evaluateToString("csv.files").split(",")));
    }

    public void parse(String delimeter, Charset charset, Consumer<Record> visit) {
        for (String csvFile : csvFiles) {
            Path file = csvFilePath.resolve(csvFile);
            if (!file.toFile().exists()) {
                throw new RuntimeException("Not found: " + file.toString());
            }
            LOG.info("Parse file: {}", csvFile);
            try (Stream<String> lines = Files.lines(file, charset)) {
                lines.skip(skipHeader).forEachOrdered(line -> visit.accept(new Record(csvFile, line, List.of(line.split(delimeter)))));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Record {
        public final String filename;
        public final String line;
        public final List<String> tokens;

        public Record(String filename, String line, List<String> tokens) {
            this.filename = filename;
            this.line = line;
            this.tokens = tokens;
        }
    }
}
