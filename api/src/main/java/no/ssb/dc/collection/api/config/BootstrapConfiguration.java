package no.ssb.dc.collection.api.config;

import no.ssb.dc.collection.api.config.internal.DynamicProxy;

import java.util.Collections;
import java.util.Map;

@Name("bootstrap")
@Prefix("")
@RequiredKeys({
        "action"
})
public interface BootstrapConfiguration extends BaseConfiguration {

    @Property("action")
    Boolean hasAction();

    @Property("action")
    String action();

    @Property("target")
    Boolean hasTarget();

    @Property("target")
    String target();

    @Property("source.specification.filepath")
    Boolean hasSpecificationFilePath();

    @Property("source.specification.filepath")
    String specificationFilePath();

    @Property("source.specification.file")
    Boolean hasSpecificationFile();

    @Property("source.specification.file")
    String specificationFile();

    @Property("target.rawdata.client.provider")
    Boolean hasRawdataClientProvider();

    @Property("target.rawdata.client.provider")
    String rawdataClientProvider();

    default Boolean isHelpAction() {
        return hasAction() && action().equals("help") && !hasTarget();
    }

    default Boolean useGCSConfiguration() {
        return hasRawdataClientProvider() && rawdataClientProvider().equals("gcs");
    }

    @Override
    default Map<String, String> defaultValues() {
        return Collections.emptyMap();
    }

    static BootstrapConfiguration create() {
        return new DynamicProxy<>(BootstrapConfiguration.class).instance();
    }

    static BootstrapConfiguration create(Map<String, String> overrideValues) {
        return new DynamicProxy<>(BootstrapConfiguration.class, overrideValues).instance();
    }

}
