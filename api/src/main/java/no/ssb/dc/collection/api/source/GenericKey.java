package no.ssb.dc.collection.api.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

abstract public class GenericKey implements RepositoryKey {

    private final static Logger LOG = LoggerFactory.getLogger(GenericKey.class);

    private final Map<String, Object> values;

    public GenericKey() {
        values = new LinkedHashMap<>();
    }

    public GenericKey(Map<String, Object> values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    public static <R extends GenericKey> R create(Class<R> clazz, Map<String, Object> values) {
        try {
            Constructor<? extends GenericKey> constructor = clazz.getDeclaredConstructor(Map.class);
            return (R) constructor.newInstance(values);

        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            LOG.error("Check that you have implemented constructors in {}", clazz);
            throw new IllegalArgumentException(e);
        }
    }

    abstract public Map<String, Class<?>> keys();

    abstract public List<String> positionKeys();

    public Map<String, Object> values() {
        return values;
    }

    @Override
    public <R extends RepositoryKey> R fromByteBuffer(ByteBuffer keyBuffer) {
        Objects.requireNonNull(keyBuffer);
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
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

            } else {
                throw new UnsupportedOperationException();
            }
        }
        return (R) GenericKey.create(this.getClass(), values);
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
        Objects.requireNonNull(allocatedBuffer);
        for (Map.Entry<String, Class<?>> entry : keys().entrySet()) {
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

            } else {
                throw new UnsupportedOperationException();
            }
        }
        return allocatedBuffer.flip();
    }

    @Override
    public String toPosition() {
        Objects.requireNonNull(positionKeys());
        return positionKeys().stream().map(key -> values.get(key).toString()).collect(Collectors.joining("."));
    }

    public boolean isKeyValueEqualTo(List<String> keys, Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        GenericKey that = (GenericKey) other;
        return keys.stream().allMatch(key -> Objects.equals(values().get(key), that.values.get(key)));
    }

    public boolean isPartOf(Object other) {
        return isKeyValueEqualTo(positionKeys(), other);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericKey that = (GenericKey) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "GenericKey{" +
                "values=" + values +
                '}';
    }
}
