package no.ssb.dc.collection.api.config;

import java.util.Map;

public interface BaseConfiguration {

    Map<String, String> defaultValues();

    @Property
    Map<String, String> asMap();

}
