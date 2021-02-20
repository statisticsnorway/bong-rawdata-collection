module rawdata.collection.kostra {

    requires jdk.unsupported;
    requires java.base;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;

    requires org.slf4j;

    requires lmdbjava;
    requires org.objectweb.asm;

    requires no.ssb.rawdata.migration.onprem;
    requires no.ssb.rawdata.encryption; // used in test

    requires java.sql;
    requires org.postgresql.jdbc;
    requires com.zaxxer.hikari;

    //opens no.ssb.dc.collection.kostra; // reflection from Commons.RepositoryKey

    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires jackson.jq;

    opens no.ssb.dc.collection.kostra to com.fasterxml.jackson.databind;

    exports no.ssb.dc.collection.kostra;

}
