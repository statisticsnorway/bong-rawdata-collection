package no.ssb.dc.collection.api.config;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicProxyTest {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicProxyTest.class);

    @Disabled
    @Test
    void dynamicConfig() {
        SourceNoDbConfiguration config = SourceNoDbConfiguration.create();
        LOG.trace("{}", config.csvFiles());
    }

}
