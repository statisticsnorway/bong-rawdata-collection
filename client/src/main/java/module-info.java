module rawdata.collection.client {

    requires no.ssb.config;
    requires no.ssb.rawdata.migration.onprem;
    requires rawdata.collection.bong;
    requires rawdata.collection.kostra;

    requires no.ssb.rawdata.avro;
    requires no.ssb.rawdata.postgres;
    requires no.ssb.rawdata.kafka;

    requires com.ibm.icu;
    requires com.ibm.icu.charset;

    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.encryption;

    requires org.slf4j;

    opens no.ssb.dc.collection.client to no.ssb.rawdata.migration.onprem;

    exports no.ssb.dc.collection.client;

}
