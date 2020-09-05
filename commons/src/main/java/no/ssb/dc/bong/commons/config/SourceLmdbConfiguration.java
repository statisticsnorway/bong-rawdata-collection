package no.ssb.dc.bong.commons.config;

import no.ssb.config.DynamicConfiguration;

import java.util.Map;
import java.util.Set;

public class SourceLmdbConfiguration extends AbstractConfiguration {

    public SourceLmdbConfiguration() {
        super();
    }

    public SourceLmdbConfiguration(Map<String, String> overrideKeyValuePairs) {
        super("source.",
                Map.of(
                        "lmdb.sizeInMb", "500",
                        "queue.poolSize", "25000", // flush buffer on threshold
                        "queue.keyBufferSize", "511",
                        "queue.valueBufferSize", "2048",
                        "csv.dateFormat", "MM/dd/yyyy HH:mm:ss"
                ),
                overrideKeyValuePairs
        );
    }

    @Override
    public String name() {
        return "source-ng";
    }

    @Override
    public Set<String> requiredKeys() {
        return Set.of(
                "source.lmdb.path",
                "source.rawdata.topic",
                "source.csv.filepath",
                "source.csv.files"
        );
    }

    @Override
    public DynamicConfiguration asDynamicConfiguration() {
        return configuration;
    }

}
