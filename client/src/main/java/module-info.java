module rawdata.collection.client {

    requires no.ssb.config;
    requires rawdata.collection.api;
    requires rawdata.collection.bong;

    requires no.ssb.rawdata.avro;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.kafka;

    requires org.slf4j;

    opens no.ssb.dc.collection.client to rawdata.collection.api;

    exports no.ssb.dc.collection.client;

}
