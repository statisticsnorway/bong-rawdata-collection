package no.ssb.dc.collection.api.config;

public interface SourceConfiguration extends BaseConfiguration {

    @Property("queue.poolSize")
    Boolean hasQueuePoolSize();

    @Property("queue.poolSize")
    Integer queuePoolSize();

    @Property("queue.keyBufferSize")
    Boolean hasQueueKeyBufferSize();

    @Property("queue.keyBufferSize")
    Integer queueKeyBufferSize();

    @Property("queue.valueBufferSize")
    Boolean hasQueueValueBufferSize();

    @Property("queue.valueBufferSize")
    Integer queueValueBufferSize();

    @Property("specification.filepath")
    String specificationFilePath();

    @Property("specification.file")
    String specificationFile();

    @Property("csv.filepath")
    String filePath();

    @Property("csv.files")
    Boolean hasCsvFiles();

    @Property("csv.files")
    String csvFiles();

    @Property("rawdata.topic")
    String topic();

}
