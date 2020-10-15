package no.ssb.dc.collection.api.config.internal;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.collection.api.config.BaseConfiguration;
import no.ssb.dc.collection.api.config.EnvironmentPrefix;
import no.ssb.dc.collection.api.config.Name;
import no.ssb.dc.collection.api.config.Namespace;
import no.ssb.dc.collection.api.config.Property;
import no.ssb.dc.collection.api.config.RequiredKeys;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A better approach would be to use ByteBuddy bytecode appender that would eliminate all reflection invocations.
 */
public class DynamicInvocationHandler implements InvocationHandler {

    static final Map<String, AbstractConfiguration> configurations = new HashMap<>();

    final Class<?> proxyClass;
    final Map<String, String> overrideValues;

    public DynamicInvocationHandler(Class<?> proxyClass, Map<String, String> overrideValues) {
        this.proxyClass = proxyClass;
        this.overrideValues = overrideValues;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.isDefault()) {
            Object nonProxyObject = getNonProxyObject(proxyClass);
            Class[] parameterTypes = args == null ? new Class[]{} : List.of(args).stream().map(Object::getClass).toArray(Class[]::new);
            Method defaultMethod = nonProxyObject.getClass().getDeclaredMethod(method.getName(), parameterTypes);
            return defaultMethod.invoke(nonProxyObject, args);
        }

        if (args != null) {
            throw new RuntimeException("Method with arguments is not supported: " + method);
        }

        Name name = proxyClass.getDeclaredAnnotation(Name.class);
        if (name == null || name.value().isBlank()) {
            throw new RuntimeException("Name annotation is not declared on: " + proxyClass);
        }

        Property property = method.getAnnotation(Property.class);
        if (property == null) {
            throw new RuntimeException("Property annotation is not declared on: " + method);
        }

        AbstractConfiguration configuration = computeConfigurationOrGet(name.value(), proxyClass);
        DynamicConfiguration dynamicConfiguration = configuration.asDynamicConfiguration();

        if (method.getName().startsWith("asMap")) {
            return dynamicConfiguration.asMap();
        }

        if (method.getName().startsWith("has")) {
            String value = dynamicConfiguration.evaluateToString(property.value());
            return Optional.ofNullable(value).filter(f -> !"".equalsIgnoreCase(f)).isPresent();
        }

        if (dynamicConfiguration.evaluateToString(property.value()) == null) {
            throw new IllegalStateException("Property not found: " + proxyClass.getSimpleName() + ": " + property.value());
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == String.class) {
            return dynamicConfiguration.evaluateToString(property.value());

        } else if (returnType == Integer.class) {
            return dynamicConfiguration.evaluateToInt(property.value());

        } else if (returnType == Long.class) {
            return Integer.valueOf(dynamicConfiguration.evaluateToInt(property.value())).longValue();

        } else if (returnType == Boolean.class) {
            return dynamicConfiguration.evaluateToBoolean(property.value());

        } else {
            throw new UnsupportedOperationException(String.format("Return type '%s' is NOT supported!", returnType));
        }
    }

    AbstractConfiguration computeConfigurationOrGet(String name, Class<?> configurationClass) {
        if (configurations.containsKey(name)) {
            return configurations.get(name);
        }

        final String environmentPrefix = configurationClass.isAnnotationPresent(EnvironmentPrefix.class) ?
                configurationClass.getDeclaredAnnotation(EnvironmentPrefix.class).value() :
                null;

        final String namespace = configurationClass.isAnnotationPresent(Namespace.class) ?
                configurationClass.getDeclaredAnnotation(Namespace.class).value().isBlank() ?
                        "" :
                        configurationClass.getDeclaredAnnotation(Namespace.class).value() + "." : "";

        final Map<String, String> defaultValues = defaultValues(configurationClass);

        AbstractConfiguration abstractConfiguration = new AbstractConfiguration(environmentPrefix, namespace, defaultValues, overrideValues) {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Set<String> requiredKeys() {
                return new HashSet<>(Arrays.asList(configurationClass.getDeclaredAnnotation(RequiredKeys.class).value()));
            }

            @Override
            public DynamicConfiguration asDynamicConfiguration() {
                return configuration;
            }
        };

        configurations.put(name, abstractConfiguration);

        return abstractConfiguration;
    }

    @SuppressWarnings("rawtypes")
    Map<String, String> defaultValues(Class clazz) {
        BaseConfiguration baseConfiguration = (BaseConfiguration) getNonProxyObject(clazz);
        return baseConfiguration.defaultValues();
    }

    // Zero Turnaround hack that bypasses the proxy for invocation of interface default method
    // https://www.jrebel.com/blog/java-proxies-default-methods-and-method-handles
    @SuppressWarnings("rawtypes")
    Object getNonProxyObject(Class clazz) {
        return Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        final Class declaringClass = method.getDeclaringClass();
                        Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class);
                        constructor.setAccessible(true);
                        return constructor.newInstance(declaringClass)
                                .unreflectSpecial(method, declaringClass)
                                .bindTo(proxy)
                                .invokeWithArguments(args);
                    }
                    return invoke(proxy, method, args); // fallback to proxy invocation
                }
        );
    }
}
