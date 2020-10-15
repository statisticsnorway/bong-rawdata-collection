package no.ssb.dc.collection.api.config.internal;

import no.ssb.config.StoreBasedDynamicConfiguration;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

abstract public class AbstractConfiguration implements Configuration {

    protected final StoreBasedDynamicConfiguration configuration;

    protected AbstractConfiguration(String environmentPrefix,
                                    String namespace,
                                    Map<String, String> defaultKeyValuePairs,
                                    Map<String, String> overrideKeyValuePairs) {

        Objects.requireNonNull(namespace);
        Objects.requireNonNull(defaultKeyValuePairs);

        // set default config
        Map<String, String> defaultConfiguration = new LinkedHashMap<>(
                new StoreBasedDynamicConfiguration.Builder()
                        .values(Configuration.convertMapToKeyValuePairs(defaultKeyValuePairs))
                        .build().asMap()
        );

        // get override config from environment variables
        StoreBasedDynamicConfiguration.Builder overrideConfigurationBuilder = new StoreBasedDynamicConfiguration.Builder();
        if (environmentPrefix != null) {
            overrideConfigurationBuilder.environment(environmentPrefix);
        }

        overrideConfigurationBuilder
                .systemProperties()
                .values(Configuration.convertMapToKeyValuePairs(overrideKeyValuePairs));

        Map<String, String> overrideConfiguration = overrideConfigurationBuilder.build().asMap();

        // validate required keys: either we got an override or we have a valid fallback key
        LinkedHashSet<String> validateConfiguration = new LinkedHashSet<>(requiredKeys());
        for (String key : requiredKeys()) {
            if (overrideConfiguration.containsKey(key)) {
                validateConfiguration.remove(key);
            } else if (defaultConfiguration.containsKey(key.replace(namespace, ""))) {
                validateConfiguration.remove(key);
            }
        }
        if (!validateConfiguration.isEmpty()) {
            throw new IllegalArgumentException("Missing configuration! Required variables: [" + String.join(", ", validateConfiguration) + "]");
        }

        // merge configuration into a map
        overrideConfiguration.forEach((name, value) -> {
            if (name.startsWith(namespace)) {
                String realName = name.replace(namespace, "");
                defaultConfiguration.put(realName, value);
            }
        });

        // build final configuration
        configuration = new StoreBasedDynamicConfiguration.Builder()
                .values(Configuration.convertMapToKeyValuePairs(defaultConfiguration))
                .build();
    }


}
