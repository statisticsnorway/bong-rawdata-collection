package no.ssb.dc.bong.commons.rawdata;

import java.util.Map;
import java.util.function.BiConsumer;

public interface BufferedWriter extends AutoCloseable {

    void commitQueue();

    <K extends RepositoryKey> void writeRecord(K repositoryKey, String line);

    <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> visit);
}
