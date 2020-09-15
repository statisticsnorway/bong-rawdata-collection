package no.ssb.dc.bong.rema;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.AbstractConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class SourceRemaConfiguration extends AbstractConfiguration {

    public SourceRemaConfiguration() {
        this(new LinkedHashMap<>());
    }

    public SourceRemaConfiguration(Map<String, String> overrideKeyValuePairs) {
        super("source.",
                Map.of(
                        "root.path", "/source",
                        "queue.capacity", "1000" // flush buffer on threshold
                ),
                overrideKeyValuePairs
        );
    }

    @Override
    public String name() {
        return "source-rema";
    }

    @Override
    public Set<String> requiredKeys() {
        return Set.of(
                "source.year",
                "source.month"
        );
    }

    @Override
    public DynamicConfiguration asDynamicConfiguration() {
        return configuration;
    }

}
