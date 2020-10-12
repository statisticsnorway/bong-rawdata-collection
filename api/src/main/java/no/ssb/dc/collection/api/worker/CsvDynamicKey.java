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
import java.util.stream.Collectors;

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

    public Map<String, Class<?>> keys() {
        return specification.groupByColumns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().type, (e1, e2) -> e2, LinkedHashMap::new));
    }

    public Map<String, Object> values() {
        return values;
    }

    public List<String> positionKeys() {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, CsvSpecification.Position> entry : specification.positionKeys.entrySet()) {
            if (entry.getValue() instanceof CsvSpecification.PositionColumnKey) {
                keys.add(entry.getKey());

            } else if (entry.getValue() instanceof CsvSpecification.PositionColumnFunction) {
                keys.add(FUNCTION_SPECIFIER + entry.getKey());

            } else {
                throw new UnsupportedOperationException();
            }
        }
        return keys;
    }

    @Override
    public <R extends RepositoryKey> R fromByteBuffer(ByteBuffer keyBuffer) {
        return fromByteBuffer(null, keyBuffer);
    }

    @Override
    public <R extends RepositoryKey> R fromByteBuffer(CsvSpecification specification, ByteBuffer keyBuffer) {
        Objects.requireNonNull(keyBuffer);
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        // PAY ATTENTION TO .keys() and resolve sorted keys per record
        for (Map.Entry<String, Class<?>> entry : this.keys().entrySet()) {
            if (entry.getValue() == String.class) {
                int stringLength = keyBuffer.getInt();
                byte[] stringBytes = new byte[stringLength];
                keyBuffer.get(stringBytes);
                values.put(entry.getKey(), new String(stringBytes, StandardCharsets.UTF_8));

            } else if (entry.getValue() == Long.class) {
                Long longValue = keyBuffer.getLong();
                values.put(entry.getKey(), longValue);

            } else if (entry.getValue() == Integer.class) {
                Integer intValue = keyBuffer.getInt();
                values.put(entry.getKey(), intValue);

            } else if (entry.getValue() == Date.class) {
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
        for (Map.Entry<String, Class<?>> entry : keys().entrySet()) {
            if (!values.containsKey(entry.getKey())) {
                throw new IllegalStateException("Missing GenericKey value for: " + entry.getKey());
            }
            if (entry.getValue() == String.class) {
                String stringValue = (String) values.get(entry.getKey());
                byte[] stringBytes = stringValue.getBytes(StandardCharsets.UTF_8);
                allocatedBuffer.putInt(stringBytes.length);
                allocatedBuffer.put(stringBytes);

            } else if (entry.getValue() == Long.class) {
                Long longValue = (Long) values.get(entry.getKey());
                allocatedBuffer.putLong(longValue);

            } else if (entry.getValue() == Integer.class) {
                Integer intValue = (Integer) values.get(entry.getKey());
                allocatedBuffer.putInt(intValue);

            } else if (entry.getValue() == Date.class) {
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
        Objects.requireNonNull(positionKeys());
        List<String> positionKeys = new ArrayList<>();
        for (String key : positionKeys()) {
            if (key.startsWith(FUNCTION_SPECIFIER)) {
                CsvSpecification.PositionColumnFunction function = getFunction(key);
                Object generatedValue;
                switch (function.generator) {
                    case SEQUENCE -> generatedValue = SequenceGenerator.next();
                    case ULID -> generatedValue = ULIDGenerator.toUUID(ULIDGenerator.generate());
                    case UUID -> generatedValue = UUIDGenerator.generate();
                    default -> throw new RuntimeException("Function type " + function.generator + " NOT supported!");
                }
                positionKeys.add(generatedValue.toString());
            } else {
                Object value = values.get(key);
                positionKeys.add(value.toString());
            }
        }
        return String.join(".", positionKeys);
    }

    public CsvSpecification.PositionColumnFunction getFunction(String key) {
        return (CsvSpecification.PositionColumnFunction) specification.positionKeys.get(key.replace(FUNCTION_SPECIFIER, ""));
    }

    public boolean isKeyValueEqualTo(List<String> keys, Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        CsvDynamicKey that = (CsvDynamicKey) other;
        return keys.stream().allMatch(key -> Objects.equals(values().get(key), that.values.get(key)));
    }

    public boolean isPartOf(Object other) {
        return isKeyValueEqualTo(positionKeys(), other);
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
