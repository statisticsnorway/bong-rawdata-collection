package no.ssb.dc.collection.api.config.internal;

import no.ssb.config.DynamicConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Configuration {

    String name();

    Set<String> requiredKeys();

    DynamicConfiguration asDynamicConfiguration();

    static String[] convertMapToKeyValuePairs(Map<String, String> map) {
        List<String> keyValuePair = new ArrayList<>();
        map.forEach((key, value) -> {
            keyValuePair.add(key);
            keyValuePair.add(value);
        });
        return keyValuePair.toArray(new String[0]);
    }

}
