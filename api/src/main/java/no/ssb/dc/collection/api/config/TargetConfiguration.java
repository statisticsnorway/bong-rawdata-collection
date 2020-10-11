package no.ssb.dc.collection.api.config;

public interface TargetConfiguration extends BaseConfiguration {

    @Property("rawdata.topic")
    String topic();

    @Property("rawdata.client.provider")
    String rawdataClientProvider();

    @Property("rawdata.encryptionKey")
    Boolean hasRawdataEncryptionKey();

    @Property("rawdata.encryptionKey")
    String rawdataEncryptionKey();

    @Property("rawdata.encryptionSalt")
    Boolean hasRawdataEncryptionSalt();

    @Property("rawdata.encryptionSalt")
    String rawdataEncryptionSalt();

    @Property("local-temp-folder")
    String localTempFolder();

    @Property("avro-file.max.seconds")
    Integer avroFileMaxSeconds();

    @Property("avro-file.max.bytes")
    Long avroMaxBytesInMiB();

    @Property("avro-file.sync.interval")
    Long avroFileSyncInterval();


}
