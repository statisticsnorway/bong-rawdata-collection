package no.ssb.dc.collection.kag;

import no.ssb.dc.collection.api.source.GenericKey;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OmkodingskatalogKey extends GenericKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();
    static final List<String> positionKeys = new ArrayList<>();

    static {
        keys.put("Fskolenr", Long.class);
        keys.put("Skolekom", Long.class);

        positionKeys.add("Fskolenr");
        positionKeys.add("Skolekom");
    }

    public OmkodingskatalogKey() {
        super();
    }

    public OmkodingskatalogKey(Map<String, Object> values) {
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
