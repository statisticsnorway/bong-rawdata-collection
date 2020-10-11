package no.ssb.dc.collection.api.csv;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class CsvReader {

    private static final Logger LOG = LoggerFactory.getLogger(CsvReader.class);

    private final Path csvFilePath;
    private final List<String> csvFiles = new ArrayList<>();

    public CsvReader(SourceConfiguration configuration, CsvSpecification specification) {
        String filepath = configuration.filePath();
        if (filepath.contains("$PROJECT_DIR")) {
            filepath = filepath.replace("$PROJECT_DIR", Paths.get(".").normalize().resolve(filepath).toString());
        }

        String sourceFiles = configuration.hasCsvFiles() ? configuration.csvFiles() : specification.fileDescriptor.files;

        this.csvFilePath = Paths.get(filepath);
        List<String> csvFiles = List.of(sourceFiles.split(","));

        boolean checkIfAllFilesExists = csvFiles.stream().allMatch(filename -> this.csvFilePath.resolve(filename).toFile().isFile());
        if (csvFiles.isEmpty() || !checkIfAllFilesExists) {
            throw new IllegalStateException("CsvFiles not found: " + (csvFiles.isEmpty() || (csvFiles.size() == 1 && csvFiles.get(0).isBlank()) ? "No files to process!" : String.join(",", csvFiles)));
        }
        this.csvFiles.addAll(csvFiles);
    }

    public void parse(char delimiter, Charset charset, Consumer<CsvParser.Record> record) {
        for (String csvFile : csvFiles) {
            Path file = csvFilePath.resolve(csvFile);
            if (!file.toFile().exists()) {
                throw new RuntimeException("Not found: " + file.toString());
            }
            LOG.info("Parse file: {}", csvFile);
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile()), charset));
                CsvParser parser = new CsvParser(reader, csvFilePath.toString(), file.getFileName().toString(), delimiter);
                parser.parse(record);

            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
