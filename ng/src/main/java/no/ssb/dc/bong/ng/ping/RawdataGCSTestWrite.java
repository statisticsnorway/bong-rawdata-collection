package no.ssb.dc.bong.ng.ping;

import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.rawdata.BufferedRawdataProducer;
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
                try (BufferedRawdataProducer bufferedRawdataProducer = new BufferedRawdataProducer(10, producer, "PASSWORD".toCharArray(), "SALT".getBytes())) {
                    bufferedRawdataProducer.produce(messageBuilder.build());
                    LOG.trace("Published: 1");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
