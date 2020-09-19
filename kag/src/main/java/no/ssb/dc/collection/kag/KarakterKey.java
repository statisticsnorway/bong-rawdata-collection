package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.source.DynamicKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KarakterKey extends DynamicKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();
    static final List<String> positionKeys = new ArrayList<>();

    static {
        keys.put("filename", String.class);
        keys.put("fileId", Long.class);
        keys.put("fnr", Integer.class);
        keys.put("rowId", Long.class);

        positionKeys.add("fileId");
        positionKeys.add("fnr");
    }

    public KarakterKey() {
        super();
    }

    public KarakterKey(Map<String, Object> values) {
        super(values);
    }

    @Override
    public Map<String, Class<?>> keys() {
        return keys;
    }

    @Override
    public List<String> positionKeys() {
        return positionKeys;
    }

    public boolean isPartOGroup(Object other) {
        return isKeyValueEqualTo(positionKeys, other);
    }
}
