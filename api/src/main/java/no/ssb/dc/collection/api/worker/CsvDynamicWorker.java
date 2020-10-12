package no.ssb.dc.collection.api.worker;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.SourceNoDbConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.source.AbstractCsvRepository;
import no.ssb.dc.collection.api.source.CsvWorker;
import no.ssb.dc.collection.api.source.LmdbCsvRepository;
import no.ssb.dc.collection.api.source.PostgresCsvRepository;
import no.ssb.dc.collection.api.source.SimpleCsvRepository;
import no.ssb.dc.collection.api.utils.ConversionUtils;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class CsvDynamicWorker implements CsvWorker<CsvDynamicKey> {

    private final SourceConfiguration sourceConfiguration;
    private final TargetConfiguration targetConfiguration;
    private final CsvSpecification specification;
    private final AbstractCsvRepository<CsvDynamicKey> csvRepository;

    public CsvDynamicWorker(SourceConfiguration sourceConfiguration, TargetConfiguration targetConfiguration, CsvSpecification specification) {
        this.sourceConfiguration = sourceConfiguration;
        this.targetConfiguration = targetConfiguration;
        this.specification = specification;
        if (sourceConfiguration instanceof SourceNoDbConfiguration) {
            this.csvRepository = createSimpleCsvRepository();
        } else if (sourceConfiguration instanceof SourceLmdbConfiguration) {
            this.csvRepository = createLmdbCsvRepository();
        } else if (sourceConfiguration instanceof SourcePostgresConfiguration) {
            this.csvRepository = createPostgresCsvRepository();
        } else {
            throw new IllegalStateException();
        }
    }

    private AbstractCsvRepository<CsvDynamicKey> createSimpleCsvRepository() {
        return new SimpleCsvRepository<>(
                (SourceNoDbConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                CsvDynamicKey.class,
                CsvDynamicKey::isPartOf,
                this::createDynamicKey
        );
    }

    private AbstractCsvRepository<CsvDynamicKey> createLmdbCsvRepository() {
        return new LmdbCsvRepository<>(
                (SourceLmdbConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                CsvDynamicKey.class,
                CsvDynamicKey::isPartOf
        );
    }

    private AbstractCsvRepository<CsvDynamicKey> createPostgresCsvRepository() {
        return new PostgresCsvRepository<>(
                (SourcePostgresConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                CsvDynamicKey.class,
                CsvDynamicKey::isPartOf
        );
    }

    @Override
    public void prepare() {
        csvRepository.prepare(this::createDynamicKey);
    }

    CsvDynamicKey createDynamicKey(CsvParser.Record csvRecord) {
        Map<String, Object> values = new LinkedHashMap<>();

        // TODO add function for uniqueness

        // iterate groupBy and produce group key with values
        specification.groupByColumns.forEach((name, column) -> {
            if (!csvRecord.headers.containsKey(name)) {
                throw new RuntimeException("Failed to resolve header column: " + name);
            }
            Map.Entry<Integer, String> entryHeaderWithColumnPositionAndAvroFieldName = csvRecord.headers.get(name);
            int columnPosition = entryHeaderWithColumnPositionAndAvroFieldName.getKey();

            String value = csvRecord.tokens.get(columnPosition);

            Object convertedValue;
            if (column.type == String.class) {
                convertedValue = value;

            } else if (column.type == Long.class) {
                convertedValue = ConversionUtils.toLong(value);

            } else if (column.type == Integer.class) {
                convertedValue = ConversionUtils.toInteger(value);

            } else if (column.type == Date.class) {
                convertedValue = ConversionUtils.toDate(value, column.format);

            } else {
                throw new UnsupportedOperationException();
            }

            values.put(name, convertedValue);
        });
        return CsvDynamicKey.create(specification, values);
    }

    @Override
    public void produce() {
        csvRepository.produce();
    }

    @Override
    public void close() {
        try {
            csvRepository.close();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }
}
