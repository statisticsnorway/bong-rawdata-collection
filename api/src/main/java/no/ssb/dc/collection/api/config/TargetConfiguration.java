package no.ssb.dc.collection.api.config;

import java.util.Map;

abstract public class TargetConfiguration extends AbstractConfiguration {

    public TargetConfiguration() {
        super();
    }

    protected TargetConfiguration(String prefix, Map<String, String> defaultKeyValuePairs, Map<String, String> overrideKeyValuePairs) {
        super(prefix, defaultKeyValuePairs, overrideKeyValuePairs);
    }
}
