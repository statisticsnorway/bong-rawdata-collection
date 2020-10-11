package no.ssb.dc.collection.api.source;

import java.util.Map;
import java.util.function.BiConsumer;

public interface BufferedReadWrite extends AutoCloseable {

    void commitQueue();

    void writeHeader(String key, String value);

    <K extends RepositoryKey> void writeRecord(K repositoryKey, String line);

    void readHeader(BiConsumer<Map.Entry<String, String>, Boolean> visit);

    <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> visit);

    @Override
    void close();
}
