package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Map;

public interface BaseConfiguration {

    Map<String, String> defaultValues();

    @Property
    Map<String, String> asMap();

    static <R extends BaseConfiguration> R create(Class<R> clazz) {
        return new DynamicProxy<>(clazz).instance();
    }

    static <R extends BaseConfiguration> R create(Class<R> clazz, Map<String, String> overrideValues) {
        return new DynamicProxy<>(clazz, overrideValues).instance();
    }

}
