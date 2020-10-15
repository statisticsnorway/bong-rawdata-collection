package no.ssb.dc.collection.api.config.internal;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapBuilder {

    final Map<String, String> map = new LinkedHashMap<>();

    private MapBuilder() {
    }

    public static MapBuilder create() {
        return new MapBuilder();
    }

    /**
     * Generalized values are common defaults
     *
     * @param key
     * @param value
     * @return
     */
    public MapBuilder values(String key, String value) {
        map.put(key, value);
        return this;
    }

    /**
     * Specialized values are specific defaults for a given callee
     *
     * @param key
     * @param value
     * @return
     */
    public MapBuilder specialized(String key, String value) {
        map.put(key, value);
        return this;
    }

    /**
     * Merge with another map
     *
     * @param other
     * @return
     */
    public MapBuilder defaults(Map<String, String> other) {
        map.putAll(other);
        return this;
    }

    public Map<String, String> build() {
        return map;
    }
}
