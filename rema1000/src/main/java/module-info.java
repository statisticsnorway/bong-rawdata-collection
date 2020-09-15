module bong.rema {

    requires jdk.unsupported;
    requires java.base;

    requires no.ssb.service.provider.api;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.rawdata.encryption;

    requires org.slf4j;

    requires bong.commons;

    opens no.ssb.dc.bong.rema;

    exports no.ssb.dc.bong.rema;
}
