package no.ssb.dc.collection.api.worker;

import no.ssb.dc.collection.api.source.RepositoryKey;
import no.ssb.dc.collection.api.utils.ULIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CsvDynamicKey implements RepositoryKey {

    private static final Logger LOG = LoggerFactory.getLogger(CsvDynamicKey.class);

    private static final String FUNCTION_SPECIFIER = "FUNCTION::";

    private final CsvSpecification specification;
    private final Map<String, Object> values;

    public CsvDynamicKey(CsvSpecification specification) {
        this.specification = specification;
        this.values = new LinkedHashMap<>();
    }

    public CsvDynamicKey(CsvSpecification specification, Map<String, Object> values) {
        this.specification = specification;
        this.values = values;
    }

    public static CsvDynamicKey create(CsvSpecification specification, Map<String, Object> values) {
        return new CsvDynamicKey(specification, values);
    }

    public Map<String, Object> values() {
        return values;
    }

    @Override
    public <R extends RepositoryKey> R fromByteBuffer(ByteBuffer keyBuffer) {
        return fromByteBuffer(null, keyBuffer);
    }

    @Override
    public <R extends RepositoryKey> R fromByteBuffer(CsvSpecification specification, ByteBuffer keyBuffer) {
        Objects.requireNonNull(keyBuffer);
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();

        for (Map.Entry<String, CsvSpecification.Key> entry : specification.columns.keys().entrySet()) {
            if (entry.getValue().type == String.class) {
                int stringLength = keyBuffer.getInt();
                byte[] stringBytes = new byte[stringLength];
                keyBuffer.get(stringBytes);
                values.put(entry.getKey(), new String(stringBytes, StandardCharsets.UTF_8));

            } else if (entry.getValue().type == Long.class) {
                Long longValue = keyBuffer.getLong();
                values.put(entry.getKey(), longValue);

            } else if (entry.getValue().type == Integer.class) {
                Integer intValue = keyBuffer.getInt();
                values.put(entry.getKey(), intValue);

            } else if (entry.getValue().type == Date.class) {
                Long longValue = keyBuffer.getLong();
                values.put(entry.getKey(), longValue);

            } else {
                throw new UnsupportedOperationException();
            }
        }

        return (R) CsvDynamicKey.create(specification, values);
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
        Objects.requireNonNull(allocatedBuffer);
        for (Map.Entry<String, CsvSpecification.Key> entry : specification.columns.keys().entrySet()) {
            if (!values.containsKey(entry.getKey())) {
                throw new IllegalStateException("Missing GenericKey value for: " + entry.getKey());
            }
            if (entry.getValue().type == String.class) {
                String stringValue = (String) values.get(entry.getKey());
                byte[] stringBytes = stringValue.getBytes(StandardCharsets.UTF_8);
                allocatedBuffer.putInt(stringBytes.length);
                allocatedBuffer.put(stringBytes);

            } else if (entry.getValue().type == Long.class) {
                Long longValue = (Long) values.get(entry.getKey());
                allocatedBuffer.putLong(longValue);

            } else if (entry.getValue().type == Integer.class) {
                Integer intValue = (Integer) values.get(entry.getKey());
                allocatedBuffer.putInt(intValue);

            } else if (entry.getValue().type == Date.class) {
                Date dateValue = (Date) values.get(entry.getKey());
                allocatedBuffer.putLong(dateValue.getTime());

            } else {
                throw new UnsupportedOperationException("Value type not supported: " + entry.getKey());
            }
        }
        return allocatedBuffer.flip();
    }

    @Override
    public String toPosition() {
        Objects.requireNonNull(specification);
        List<String> positionValues = new ArrayList<>();
        for (Map.Entry<String, CsvSpecification.Key> entry : specification.columns.positionKeys().entrySet()) {
            if (entry.getValue().isColumn()) {
                Object value = values.get(entry.getKey());
                positionValues.add(value.toString());

            } else if (entry.getValue().isFunction()) {
                Object generatedValue;
                switch (entry.getValue().asFunction().generator) {
                    case SEQUENCE -> generatedValue = SequenceGenerator.next();
                    case ULID -> generatedValue = ULIDGenerator.toUUID(ULIDGenerator.generate());
                    case UUID -> generatedValue = UUIDGenerator.generate();
                    default -> throw new RuntimeException("Function type " + entry.getValue().asFunction().generator + " NOT supported!");
                }
                positionValues.add(generatedValue.toString());

            } else {
                throw new UnsupportedOperationException();
            }
        }
        return String.join(".", positionValues);
    }

    public boolean isKeyValueEqualTo(List<String> keys, Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        CsvDynamicKey that = (CsvDynamicKey) other;
        return keys.stream().allMatch(key -> Objects.equals(values().get(key), that.values.get(key)));
    }

    public boolean isPartOf(Object other) {
        List<String> compareKeys = new ArrayList<>(specification.columns.groupByKeys().keySet());
        return isKeyValueEqualTo(compareKeys, other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CsvDynamicKey that = (CsvDynamicKey) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(specification, values);
    }

    @Override
    public String toString() {
        return "CsvDynamicKey{" +
                "values=" + values +
                '}';
    }
}
