package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.source.GenericKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BefolkningKey extends GenericKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();
    static final List<String> positionKeys = new ArrayList<>();
    static final AtomicLong seq = new AtomicLong();

    static {
        keys.put("filename", String.class);
        keys.put("Fnr", Long.class);
        keys.put("lineIndex", Long.class);

        positionKeys.add("Fnr");
    }

    public BefolkningKey() {
        super();
    }

    public BefolkningKey(Map<String, Object> values) {
        super(values);
        values.put("lineIndex", seq.incrementAndGet());
    }

    @Override
    public Map<String, Class<?>> keys() {
        return keys;
    }

    @Override
    public List<String> positionKeys() {
        return positionKeys;
    }
}
