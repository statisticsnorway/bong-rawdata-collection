module bong.commons {

    requires jdk.unsupported;
    requires java.base;
    requires java.logging;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.encryption;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;

    requires lmdbjava;
    requires org.objectweb.asm;

    requires java.sql;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;

    opens no.ssb.dc.bong.commons.postgres.init;

    exports no.ssb.dc.bong.commons.config;
    exports no.ssb.dc.bong.commons.csv;
    exports no.ssb.dc.bong.commons.lmdb;
    exports no.ssb.dc.bong.commons.postgres;
    exports no.ssb.dc.bong.commons.rawdata;

}
