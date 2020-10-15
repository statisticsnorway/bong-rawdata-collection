package no.ssb.dc.collection.api.csv;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.source.BufferedReadWrite;
import no.ssb.dc.collection.api.source.RepositoryKey;
import no.ssb.dc.collection.api.utils.EncodingUtils;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvReader {

    private static final Logger LOG = LoggerFactory.getLogger(CsvReader.class);

    public static final AtomicLong csvReadLines = new AtomicLong(0);

    private final Path csvFilePath;
    private final CsvSpecification specification;
    private final List<String> csvFiles = new ArrayList<>();
    private final int dryRun;
    private final ByteBuffer encodingBuffer;

    public CsvReader(SourceConfiguration configuration, CsvSpecification specification) {
        csvFilePath = Paths.get(configuration.filePath()).normalize().toAbsolutePath();
        this.specification = specification;
        String sourceFiles = configuration.hasCsvFiles() ? configuration.csvFiles() : specification.fileDescriptor.files;
        if (sourceFiles == null) {
            throw new IllegalArgumentException("No source files to process: " + sourceFiles);
        }
        LOG.info("Process source files: {}", sourceFiles);
        List<String> csvFiles = List.of(sourceFiles.split(","));
        boolean checkIfAllFilesExists = csvFiles.stream().allMatch(filename -> Files.isReadable(csvFilePath.resolve(filename)));
        if (csvFiles.isEmpty() || !checkIfAllFilesExists) {
            throw new IllegalStateException("CsvFiles not found: " + (csvFiles.isEmpty() || (csvFiles.size() == 1 && csvFiles.get(0).isBlank()) ? "No files to process!" : String.join(",", csvFiles)));
        }
        this.csvFiles.addAll(csvFiles);
        this.dryRun = configuration.hasDryRun() ? configuration.dryRun() : -1;
        this.encodingBuffer = ByteBuffer.allocateDirect(configuration.queueValueBufferSize());
    }

    public List<String> getFiles() {
        return csvFiles;
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
                CsvParser parser = new CsvParser(reader, csvFilePath.toString(), csvFiles, file.getFileName().toString(), delimiter, dryRun);
                parser.parse(record);

            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public <K extends RepositoryKey> void readAndBufferedWrite(BufferedReadWrite bufferedReadWrite, Function<CsvParser.Record, K> produceSortableKey) {
        AtomicBoolean isHeaderRecord = new AtomicBoolean(true);
        // this call do process multiple csv files
        parse(specification.fileDescriptor.delimiter, specification.fileDescriptor.charset, record -> {
            // all files must comply with the same header, so we persist it once
            if (isHeaderRecord.get()) {
                bufferedReadWrite.writeHeader("filepath", record.filepath);
                bufferedReadWrite.writeHeader("filename", record.filename);
                bufferedReadWrite.writeHeader("recordType", record.recordType());
                bufferedReadWrite.writeHeader("csvHeader", EncodingUtils.encodeArray(new ArrayList<>(record.headers.keySet()), encodingBuffer));
                String delimiterString = Character.toString(record.delimiter);
                bufferedReadWrite.writeHeader("delimiter", delimiterString);
                bufferedReadWrite.writeHeader("avroHeader", EncodingUtils.encodeArray(record.headers.values().stream().map(Map.Entry::getValue).collect(Collectors.toList()), encodingBuffer));
                isHeaderRecord.set(false);
            }

            RepositoryKey sortableGroupKey = produceSortableKey.apply(record); // worker produces valid groupKey (CsvDynamicWorker.createDynamicKey produces a toBuffer(key))

            bufferedReadWrite.writeRecord(sortableGroupKey, record.asLine());

            if (csvReadLines.incrementAndGet() % 100000 == 0) {
                LOG.info("Source - Read lines: {}", csvReadLines.get());
            }
        });

        bufferedReadWrite.close();

        LOG.info("Source - Read Lines Total: {}", csvReadLines.get());
    }

}
