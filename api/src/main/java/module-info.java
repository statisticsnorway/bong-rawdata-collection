module rawdata.collection.api {

    requires jdk.unsupported;
    requires java.base;
    requires java.logging;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.encryption;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;

    requires commons.csv;
    requires org.apache.commons.text;

    requires lmdbjava;
    requires org.objectweb.asm;

    requires java.sql;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;

    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.dataformat.yaml;

    opens no.ssb.dc.collection.api.postgres.init;

    exports no.ssb.dc.collection.api.csv;
    exports no.ssb.dc.collection.api.config;
    exports no.ssb.dc.collection.api.jdbc;
    exports no.ssb.dc.collection.api.source;
    exports no.ssb.dc.collection.api.target;
    exports no.ssb.dc.collection.api.worker;
    exports no.ssb.dc.collection.api.utils;

}
