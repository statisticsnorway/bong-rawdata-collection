package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.source.GenericKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ResultatKey extends GenericKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();
    static final List<String> positionKeys = new ArrayList<>();

    static {
        keys.put("filename", String.class);
        keys.put("Filid", Long.class);
        keys.put("Fnr", Long.class);
        keys.put("Radnr", Long.class);

        positionKeys.add("Filid");
        positionKeys.add("Fnr");
    }

    public ResultatKey() {
        super();
    }

    public ResultatKey(Map<String, Object> values) {
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
