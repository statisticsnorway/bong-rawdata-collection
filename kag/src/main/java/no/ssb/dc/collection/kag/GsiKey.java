package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.source.GenericKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GsiKey extends GenericKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();
    static final List<String> positionKeys = new ArrayList<>();

    // TODO should probably be ulid because of inconsistent data
    static {
        keys.put("ulid", String.class);

        positionKeys.add("ulid");
    }

    public GsiKey() {
        super();
    }

    public GsiKey(Map<String, Object> values) {
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
}
