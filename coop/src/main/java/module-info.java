module bong.coop {

    requires jdk.unsupported;
    requires java.base;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;

    requires org.slf4j;

    requires lmdbjava;
    requires org.objectweb.asm;

    requires bong.commons;

    opens no.ssb.dc.bong.coop; // reflection from Commons.RepositoryKey

    exports no.ssb.dc.bong.coop;

}
