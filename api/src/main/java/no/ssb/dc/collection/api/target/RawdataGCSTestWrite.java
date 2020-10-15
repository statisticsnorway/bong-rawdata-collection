package no.ssb.dc.collection.api.target;

import no.ssb.dc.collection.api.config.ConfigurationFactory;
import no.ssb.dc.collection.api.config.Name;
import no.ssb.dc.collection.api.config.Namespace;
import no.ssb.dc.collection.api.config.RequiredKeys;
import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RawdataGCSTestWrite {

    static final Logger LOG = LoggerFactory.getLogger(RawdataGCSTestWrite.class);

    @Name("test-gcs-write")
    @Namespace("source")
    @RequiredKeys({})
    interface TestConfiguration extends SourceConfiguration {
        default Map<String, String> defaultValues() {
            return SourceConfiguration.sourceDefaultValues();
        }
    }

    public void produceRawdataToGCS(TargetConfiguration gcsConfiguration) {
        LOG.info("{}", gcsConfiguration.asMap());

        // read from local rawdata storage

        // write to remove rawdata bucket
        try (RawdataClient gcsClient = ProviderConfigurator.configure(gcsConfiguration.asMap(), gcsConfiguration.rawdataClientProvider(), RawdataClientInitializer.class)) {
            try (RawdataProducer producer = gcsClient.producer(gcsConfiguration.topic())) {
                RawdataMessage.Builder messageBuilder = producer.builder();
                messageBuilder.position("1");
                messageBuilder.put("entry", "hello".getBytes());
                try (BufferedRawdataProducer bufferedRawdataProducer = new BufferedRawdataProducer(ConfigurationFactory.createOrGet(TestConfiguration.class), gcsConfiguration, null, producer)) {
                    bufferedRawdataProducer.produce(messageBuilder.build());
                    LOG.trace("Published: 1");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
