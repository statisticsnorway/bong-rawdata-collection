package no.ssb.dc.bong.commons.source;

public interface CsvWorker<T extends RepositoryKey> extends AutoCloseable {

    void prepare();

    void produce();

    @Override
    void close();
}
