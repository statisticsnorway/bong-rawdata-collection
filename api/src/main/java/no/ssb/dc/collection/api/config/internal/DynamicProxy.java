package no.ssb.dc.collection.api.config.internal;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;

public class DynamicProxy<T> {
    final T instance;

    public DynamicProxy(Class<T> clazz) {
        this(clazz, new LinkedHashMap<>());
    }

    public T instance() {
        return instance;
    }

    @SuppressWarnings("unchecked")
    public DynamicProxy(Class<T> clazz, Map<String, String> overrideValues) {
        instance = (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[]{clazz},
                new DynamicInvocationHandler(clazz, overrideValues));
    }
}
