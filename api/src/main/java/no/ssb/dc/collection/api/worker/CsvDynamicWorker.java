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
import no.ssb.dc.collection.api.utils.ULIDGenerator;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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

        specification.columns.keys().forEach((name, columnKey) -> {
            if (!csvRecord.headers.containsKey(name) && !columnKey.isFunction()) {
                throw new RuntimeException("Failed to resolve header column: " + name);
            }

            Object convertedValue;
            if (columnKey.isColumn()) {
                Map.Entry<Integer, String> entryHeaderWithColumnPositionAndAvroFieldName = csvRecord.headers.get(name);
                int columnPosition = entryHeaderWithColumnPositionAndAvroFieldName.getKey();

                String value = csvRecord.tokens.get(columnPosition);
                convertedValue = convertStringValueAsTypeSafe(columnKey.type, value, columnKey.asColumn().format);

            } else if (columnKey.isFunction()) {
                Object generatedValue;
                switch (columnKey.asFunction().generator) {
                    case SEQUENCE -> generatedValue = SequenceGenerator.next();
                    case ULID -> generatedValue = ULIDGenerator.toUUID(ULIDGenerator.generate());
                    case UUID -> generatedValue = UUIDGenerator.generate();
                    default -> throw new RuntimeException("Function type " + columnKey.asFunction().generator + " NOT supported!");
                }
                convertedValue = convertStringValueAsTypeSafe(columnKey.type, convertGeneratedValueToString(generatedValue), null);

            } else {
                throw new UnsupportedOperationException();
            }

            values.put(name, convertedValue);
        });
        return CsvDynamicKey.create(specification, values);
    }

    private Object convertStringValueAsTypeSafe(Class<?> type, String value, String format) {
        Object convertedValue;
        if (type == String.class) {
            convertedValue = value;

        } else if (type == Long.class) {
            convertedValue = ConversionUtils.toLong(value);

        } else if (type == Integer.class) {
            convertedValue = ConversionUtils.toInteger(value);

        } else if (type == Date.class) {
            convertedValue = ConversionUtils.toDate(value, format);

        } else {
            throw new UnsupportedOperationException();
        }
        return convertedValue;
    }

    private String convertGeneratedValueToString(Object value) {
        if (value instanceof Long) {
            return value.toString();

        } else if (value instanceof UUID) {
            return value.toString();

        } else {
            throw new UnsupportedOperationException();
        }
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
