module bong.ng {

    requires jdk.unsupported;
    requires java.base;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;

    requires bong.commons;
    requires no.ssb.rawdata.encryption; // used in test

    requires org.slf4j;

    requires lmdbjava;
    requires org.objectweb.asm;

    requires java.sql;
    requires org.postgresql.jdbc;
    requires com.zaxxer.hikari;

    opens no.ssb.dc.bong.ng.repository; // reflection from Commons.RepositoryKey

    exports no.ssb.dc.bong.ng.repository;
    exports no.ssb.dc.bong.ng.ping;

}
