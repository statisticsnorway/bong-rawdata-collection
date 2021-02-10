module rawdata.collection.bong {

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

    opens no.ssb.dc.collection.bong.rema; // reflection from Commons.RepositoryKey

    exports no.ssb.dc.collection.bong.rema;

}
