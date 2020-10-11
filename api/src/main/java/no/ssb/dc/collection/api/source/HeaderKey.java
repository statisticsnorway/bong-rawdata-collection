package no.ssb.dc.collection.api.source;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HeaderKey extends GenericKey {

    static final Map<String, Class<?>> keys = new LinkedHashMap<>();

    static {
        keys.put("csvHeader", String.class); // TODO this a wrong pattern, because it only allows for a single key into the keyValue DB
    }

    public HeaderKey() {
        super();
    }

    public HeaderKey(Map<String, Object> values) {
        super(values);
    }

    public String get() {
        return (String) values().get("csvHeader");
    }

    @Override
    public Map<String, Class<?>> keys() {
        return keys;
    }

    @Override
    public List<String> positionKeys() {
        throw new UnsupportedOperationException();
    }

}
