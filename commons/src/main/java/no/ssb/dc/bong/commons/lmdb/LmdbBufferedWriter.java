package no.ssb.dc.bong.commons.lmdb;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.rawdata.BufferedWriter;
import no.ssb.dc.bong.commons.rawdata.RepositoryKey;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

public class LmdbBufferedWriter implements BufferedWriter {

    private final Queue<Map.Entry<ByteBuffer, ByteBuffer>> bufferQueue;
    private final DirectByteBufferPool keyPool;
    private final DirectByteBufferPool valuePool;
    private final LmdbEnvironment lmdbEnvironment;
    private final Dbi<ByteBuffer> dbi;

    public LmdbBufferedWriter(DynamicConfiguration configuration, LmdbEnvironment lmdbEnvironment) {
        this.lmdbEnvironment = lmdbEnvironment;
        int queuePoolSize = configuration.evaluateToInt("queue.poolSize");
        int keySize = configuration.evaluateToString("queue.keyBufferSize") == null ? 511 : configuration.evaluateToInt("queue.keyBufferSize");
        int valueSize = configuration.evaluateToString("queue.valueBufferSize") == null ? 0 : configuration.evaluateToInt("queue.valueBufferSize");
        this.bufferQueue = new LinkedBlockingDeque<>(queuePoolSize);
        this.keyPool = new DirectByteBufferPool(queuePoolSize + 1, keySize);
        this.valuePool = new DirectByteBufferPool(queuePoolSize + 1, valueSize);
        this.dbi = lmdbEnvironment.open();
    }

    @Override
    public void commitQueue() {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnWrite()) {
            Map.Entry<ByteBuffer, ByteBuffer> buffer;
            while ((buffer = bufferQueue.poll()) != null) {
                try {
                    dbi.put(txn, buffer.getKey(), buffer.getValue());
                } finally {
                    keyPool.release(buffer.getKey());
                    valuePool.release(buffer.getValue());
                }
            }
            txn.commit();
        }
    }

    @Override
    public <K extends RepositoryKey> void writeRecord(K repositoryKey, String line) {
        ByteBuffer keyBuffer = keyPool.acquire();
        repositoryKey.toByteBuffer(keyBuffer);
        ByteBuffer contentBuffer = valuePool.acquire();
        byte[] contentBytes = line.getBytes(StandardCharsets.UTF_8);
        contentBuffer.putInt(contentBytes.length);
        contentBuffer.put(contentBytes);
        contentBuffer.flip();
        if (!bufferQueue.offer(Map.entry(keyBuffer, contentBuffer))) {
            commitQueue();
            // try to re-offer buffer after queue reach max capacity
            if (!bufferQueue.offer(Map.entry(keyBuffer, contentBuffer))) {
                throw new IllegalStateException("Buffers was not added to bufferQueue!");
            }
        }
    }

    @Override
    public <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> visit) {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = dbi.iterate(txn).iterator();
            while (it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();
                ByteBuffer keyBuffer = next.key();
                K repositoryKey = RepositoryKey.fromByteBuffer(keyClass, keyBuffer);
                ByteBuffer valueBuffer = next.val();
                int contentLength = valueBuffer.getInt();
                byte[] contentBytes = new byte[contentLength];
                valueBuffer.get(contentBytes);
                String content = new String(contentBytes, StandardCharsets.UTF_8);
                visit.accept(Map.entry(repositoryKey, content), it.hasNext());
            }
        }
    }

    @Override
    public void close() {
        commitQueue();
        keyPool.close();
        valuePool.close();
    }
}
