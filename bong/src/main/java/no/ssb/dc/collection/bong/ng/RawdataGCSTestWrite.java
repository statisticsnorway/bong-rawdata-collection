package no.ssb.dc.collection.bong.ng;

import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawdataGCSTestWrite {

    static final Logger LOG = LoggerFactory.getLogger(RawdataGCSTestWrite.class);

    public void produceRawdataToGCS(TargetConfiguration gcsConfiguration) {
        LOG.info("{}", gcsConfiguration.asMap());

        // read from local rawdata storage

        // write to remove rawdata bucket
        try (RawdataClient gcsClient = ProviderConfigurator.configure(gcsConfiguration.asMap(), gcsConfiguration.asDynamicConfiguration().evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class)) {
            try (RawdataProducer producer = gcsClient.producer(gcsConfiguration.asDynamicConfiguration().evaluateToString("rawdata.topic"))) {
                RawdataMessage.Builder messageBuilder = producer.builder();
                messageBuilder.position("1");
                messageBuilder.put("entry", "hello".getBytes());
                try (BufferedRawdataProducer bufferedRawdataProducer = new BufferedRawdataProducer(gcsConfiguration.asDynamicConfiguration(), 10, producer)) {
                    bufferedRawdataProducer.produce(messageBuilder.build());
                    LOG.trace("Published: 1");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
