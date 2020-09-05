package no.ssb.dc.bong.rema;

import no.ssb.config.DynamicConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemaBongRepository {

    private static final Logger LOG = LoggerFactory.getLogger(RemaBongRepository.class);

    private final DynamicConfiguration configuration;

    public RemaBongRepository(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    void scanFileSystem() {
        LOG.trace("SourcePath: {}", configuration.evaluateToString("source.path"));
    }

    public void produceRawdata() {

    }

}
