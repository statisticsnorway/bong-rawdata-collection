module bong.client {

    requires no.ssb.config;
    requires bong.commons;
    requires bong.ng;
    requires bong.coop;

    requires no.ssb.rawdata.avro;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.kafka;

    requires org.slf4j;

    opens no.ssb.dc.bong.client to bong.commons;

    exports no.ssb.dc.bong.client;

}
