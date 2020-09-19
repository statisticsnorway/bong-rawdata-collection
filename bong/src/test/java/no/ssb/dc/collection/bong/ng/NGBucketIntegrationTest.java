package no.ssb.dc.collection.bong.ng;

import no.ssb.dc.collection.api.config.GCSConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * https://cloud.google.com/sdk/docs/proxy-settings
 *
 * https://medium.com/google-cloud/configuring-gcloud-gsutil-and-bq-to-use-proxy-servers-4f09dbaf42c2
 */
public class NGBucketIntegrationTest {

    static final Logger LOG = LoggerFactory.getLogger(NGBucketIntegrationTest.class);

    // -e BONG_gcs.service-account.key-file="/Users/oranheim/bin/ssb-team-dapla-rawdata-bong-dc28ff0c8faa.json"
    @Disabled
    @Test
    void produceRawdataToGCS() {
        GCSConfiguration gcsConfiguration = new GCSConfiguration(Map.of(
                "target.rawdata.topic", "bong-ng-test",
                "target.gcs.bucket-name", "dapla-rawdata-bong-bucket-dapla",
                "target.gcs.service-account.key-file", "/Users/oranheim/bin/ssb-team-dapla-rawdata-bong-dc28ff0c8faa.json",
                "target.local-temp-folder", "target/avro/temp",
                "rawdata.encryptionKey", "PASSWORD",
                "rawdata.encryptionSalt", "SALT"
        ));
        RawdataGCSTestWrite rawdataGCSTestWrite = new RawdataGCSTestWrite();
        rawdataGCSTestWrite.produceRawdataToGCS(gcsConfiguration);
    }
}
