package no.ssb.dc.collection.api.worker;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.SourceNoDbConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.csv.CsvParser;
import no.ssb.dc.collection.api.source.CsvHandler;
import no.ssb.dc.collection.api.source.CsvWorker;
import no.ssb.dc.collection.api.source.LmdbCsvHandler;
import no.ssb.dc.collection.api.source.PostgresCsvHandler;
import no.ssb.dc.collection.api.source.SimpleCsvHandler;
import no.ssb.dc.collection.api.utils.ConversionUtils;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class CsvDynamicWorker implements CsvWorker<CsvDynamicKey> {

    private final SourceConfiguration sourceConfiguration;
    private final TargetConfiguration targetConfiguration;
    private final CsvSpecification specification;
    private final CsvHandler<CsvDynamicKey> csvHandler;

    public CsvDynamicWorker(SourceConfiguration sourceConfiguration, TargetConfiguration targetConfiguration, CsvSpecification specification) {
        this.sourceConfiguration = sourceConfiguration;
        this.targetConfiguration = targetConfiguration;
        this.specification = specification;
        if (sourceConfiguration instanceof SourceNoDbConfiguration) {
            this.csvHandler = createSimpleCsvHandler();
        } else if (sourceConfiguration instanceof SourceLmdbConfiguration) {
            this.csvHandler = createLmdbCsvHandlerepository();
        } else if (sourceConfiguration instanceof SourcePostgresConfiguration) {
            this.csvHandler = createPostgresCsvHandler();
        } else {
            throw new IllegalStateException();
        }
    }

    private CsvHandler<CsvDynamicKey> createSimpleCsvHandler() {
        return new SimpleCsvHandler<>(
                (SourceNoDbConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                this::produceDynamicKey
        );
    }

    private CsvHandler<CsvDynamicKey> createLmdbCsvHandlerepository() {
        return new LmdbCsvHandler<>(
                (SourceLmdbConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                CsvDynamicKey.class,
                CsvDynamicKey::isPartOf
        );
    }

    private CsvHandler<CsvDynamicKey> createPostgresCsvHandler() {
        return new PostgresCsvHandler<>(
                (SourcePostgresConfiguration) sourceConfiguration,
                targetConfiguration,
                specification,
                CsvDynamicKey.class,
                CsvDynamicKey::isPartOf
        );
    }

    @Override
    public void prepare() {
        csvHandler.prepare(this::produceDynamicKey);
    }

    CsvDynamicKey produceDynamicKey(CsvParser.Record csvRecord) {
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
                    case SEQUENCE -> generatedValue = SequenceGenerator.next(SequenceGenerator.Subject.DYNAMIC_KEY);
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
        csvHandler.produce();
    }

    @Override
    public void close() {
        try {
            csvHandler.close();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }
}
