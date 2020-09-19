package no.ssb.dc.collection.api.source;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicKeyTest {

    @Test
    void name() {
        ByteBuffer fromBuffer = ByteBuffer.allocateDirect(511);
        byte[] stringType = "John Doe".getBytes();
        fromBuffer.putInt(stringType.length);
        fromBuffer.put(stringType);
        fromBuffer.putInt(25);
        fromBuffer.flip();
        DummyKey fromDummyKey = RepositoryKey.fromByteBuffer(DummyKey.class, fromBuffer);
        assertEquals("John Doe", fromDummyKey.values().get("name"));
        assertEquals(25, (Integer) fromDummyKey.values().get("age"));
        assertEquals("John Doe.25", fromDummyKey.toPosition());

        ByteBuffer toBuffer = ByteBuffer.allocateDirect(511);
        fromDummyKey.toByteBuffer(toBuffer);

        DummyKey toDummyKey = RepositoryKey.fromByteBuffer(DummyKey.class, toBuffer);
        assertEquals(fromDummyKey, toDummyKey);
    }

    static class DummyKey extends DynamicKey {

        static final Map<String, Class<?>> keys = new LinkedHashMap<>();

        static {
            keys.put("name", String.class);
            keys.put("age", Integer.class);
        }

        public DummyKey() {
            super();
        }

        public DummyKey(Map<String, Object> values) {
            super(values);
        }

        @Override
        public Map<String, Class<?>> keys() {
            return keys;
        }

        @Override
        public List<String> positionKeys() {
            return List.of("name", "age");
        }
    }
}
