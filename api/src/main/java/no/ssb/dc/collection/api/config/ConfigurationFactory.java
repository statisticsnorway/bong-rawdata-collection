package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Map;

public class ConfigurationFactory {

    public static <R extends BaseConfiguration> R createOrGet(Class<R> clazz) {
        return new DynamicProxy<>(clazz).instance();
    }

    public static <R extends BaseConfiguration> R createOrGet(Class<R> clazz, Map<String, String> overrideValues) {
        return new DynamicProxy<>(clazz, overrideValues).instance();
    }

}
