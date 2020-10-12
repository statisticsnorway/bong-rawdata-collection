package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.config.SourceConfiguration;
import no.ssb.dc.collection.api.utils.DirectByteBufferPool;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class LmdbBufferedReadWrite implements BufferedReadWrite {

    private final Queue<Map.Entry<ByteBuffer, ByteBuffer>> bufferQueue;
    private final DirectByteBufferPool keyPool;
    private final DirectByteBufferPool valuePool;
    private final LmdbEnvironment lmdbEnvironment;
    private final CsvSpecification specification;
    private final Dbi<ByteBuffer> metaDbi;
    private final Dbi<ByteBuffer> recordDbi;

    public LmdbBufferedReadWrite(SourceConfiguration configuration, LmdbEnvironment lmdbEnvironment, CsvSpecification specification) {
        this.lmdbEnvironment = lmdbEnvironment;
        this.specification = specification;
        int queuePoolSize = configuration.queuePoolSize();
        int keySize = configuration.hasQueueKeyBufferSize() ? 511 : configuration.queueKeyBufferSize();
        int valueSize = configuration.hasQueueValueBufferSize() ? 2048 : configuration.queueValueBufferSize();
        this.bufferQueue = new LinkedBlockingDeque<>(queuePoolSize);
        this.keyPool = new DirectByteBufferPool(queuePoolSize + 1, keySize);
        this.valuePool = new DirectByteBufferPool(queuePoolSize + 1, valueSize);
        this.metaDbi = lmdbEnvironment.openMetaDb();
        this.recordDbi = lmdbEnvironment.openRecordDb();
    }

    @Override
    public void commitQueue() {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnWrite()) {
            Map.Entry<ByteBuffer, ByteBuffer> buffer;
            while ((buffer = bufferQueue.poll()) != null) {
                try {
                    recordDbi.put(txn, buffer.getKey(), buffer.getValue());
                } finally {
                    keyPool.release(buffer.getKey());
                    valuePool.release(buffer.getValue());
                }
            }
            txn.commit();
        }
    }

    @Override
    public void writeHeader(String key, String value) {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnWrite()) {
            ByteBuffer keyBuffer = keyPool.acquire();
            byte[] keyBytes = key.getBytes();
            keyBuffer.putInt(key.length());
            keyBuffer.put(keyBytes);
            keyBuffer.flip();

            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(valueBytes.length + 8);
            valueBuffer.putInt(valueBytes.length);
            valueBuffer.put(valueBytes);
            valueBuffer.flip();

            try {
                metaDbi.put(txn, keyBuffer, valueBuffer);
            } finally {
                keyPool.release(keyBuffer);
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
    public void readHeader(BiConsumer<Map.Entry<String, String>, Boolean> handleRecordCallback) {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = metaDbi.iterate(txn).iterator();
            while (it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();

                ByteBuffer keyBuffer = next.key();
                int keyLength = keyBuffer.getInt();
                byte[] keyBytes = new byte[keyLength];
                keyBuffer.get(keyBytes);
                String key = new String(keyBytes);

                ByteBuffer valueBuffer = next.val();
                int valueLength = valueBuffer.getInt();
                byte[] valueBytes = new byte[valueLength];
                valueBuffer.get(valueBytes);
                String value = new String(valueBytes);
                handleRecordCallback.accept(Map.entry(key, value), it.hasNext());
            }
        }
    }

    @Override
    public <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> handleRecordCallback) {
        AtomicLong count = new AtomicLong();
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = recordDbi.iterate(txn).iterator();
            while (it.hasNext()) {
                System.out.printf("%s%n", count.incrementAndGet());
                CursorIterable.KeyVal<ByteBuffer> next = it.next();

                ByteBuffer keyBuffer = next.key();
                K repositoryKey = RepositoryKey.fromByteBuffer(keyClass, keyBuffer, specification);

                ByteBuffer contentBuffer = next.val();
                int contentLength = contentBuffer.getInt();
                byte[] contentBytes = new byte[contentLength];
                contentBuffer.get(contentBytes);
                String content = new String(contentBytes, StandardCharsets.UTF_8);

                handleRecordCallback.accept(Map.entry(repositoryKey, content), it.hasNext());
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
