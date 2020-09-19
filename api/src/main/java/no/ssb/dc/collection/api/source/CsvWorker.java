package no.ssb.dc.collection.api.source;

public interface CsvWorker<T extends RepositoryKey> extends AutoCloseable {

    void prepare();

    void produce();

    @Override
    void close();
}
