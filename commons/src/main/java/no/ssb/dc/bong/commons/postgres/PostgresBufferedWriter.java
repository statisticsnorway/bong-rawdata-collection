package no.ssb.dc.bong.commons.postgres;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.lmdb.DirectByteBufferPool;
import no.ssb.dc.bong.commons.rawdata.FixedThreadPool;
import no.ssb.dc.bong.commons.rawdata.RepositoryKey;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class PostgresBufferedWriter implements AutoCloseable {

    final FixedThreadPool threadPool;
    final Queue<Map.Entry<ByteBuffer, ByteBuffer>> bufferQueue;
    final DirectByteBufferPool keyPool;
    final DirectByteBufferPool valuePool;
    final PostgresTransactionFactory transactionFactory;
    final String topic;
    final AtomicBoolean isTableCreated = new AtomicBoolean(false);

    public PostgresBufferedWriter(DynamicConfiguration configuration, PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
        this.threadPool = FixedThreadPool.newInstance();
        this.topic = configuration.evaluateToString("rawdata.topic");
        int queuePoolSize = configuration.evaluateToInt("queue.poolSize");
        int keySize = configuration.evaluateToString("queue.keyBufferSize") == null ? 511 : configuration.evaluateToInt("queue.keyBufferSize");
        int valueSize = configuration.evaluateToString("queue.valueBufferSize") == null ? 0 : configuration.evaluateToInt("queue.valueBufferSize");
        this.bufferQueue = new LinkedBlockingDeque<>(queuePoolSize);
        this.keyPool = new DirectByteBufferPool(queuePoolSize + 1, keySize);
        this.valuePool = new DirectByteBufferPool(queuePoolSize + 1, valueSize);
    }

    void createTopicIfNotExists(String topic, boolean dropAndCreateDatabase) {
        if (!isTableCreated.get() && (!transactionFactory.checkIfTableTopicExists(topic, "bong_item") || dropAndCreateDatabase)) {
            PostgresDataSource.dropOrCreateDatabase(transactionFactory, topic);
            isTableCreated.set(true);
        } else if (!dropAndCreateDatabase) {
            isTableCreated.set(true);
        }
    }

    public void commitQueue() {
        createTopicIfNotExists(topic, true);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            Map.Entry<ByteBuffer, ByteBuffer> buffer;
            PreparedStatement ps = transaction.connection().prepareStatement(String.format("INSERT INTO \"%s_bong_item\" (key, value, ts) VALUES (?, ?, ?)", topic));
            while ((buffer = bufferQueue.poll()) != null) {
                try {
                    byte[] key = new byte[buffer.getKey().remaining()];
                    byte[] value = new byte[buffer.getValue().remaining()];
                    buffer.getKey().get(key);
                    buffer.getValue().get(value);
                    ps.setBytes(1, key);
                    ps.setBytes(2, value);
                    ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                    ps.addBatch();

                } finally {
                    keyPool.release(buffer.getKey());
                    valuePool.release(buffer.getValue());
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <K extends RepositoryKey> void writeRecord(K key, String value) {
        ByteBuffer keyBuffer = keyPool.acquire();
        key.toByteBuffer(keyBuffer);
        ByteBuffer contentBuffer = valuePool.acquire();
        byte[] contentBytes = value.getBytes(StandardCharsets.UTF_8);
        contentBuffer.putInt(contentBytes.length);
        contentBuffer.put(contentBytes);
        contentBuffer.flip();
        if (!bufferQueue.offer(Map.entry(keyBuffer, contentBuffer))) {
            commitQueue();
            // re-offer buffer after queue reach max capacity
            if (!bufferQueue.offer(Map.entry(keyBuffer, contentBuffer))) {
                throw new IllegalStateException("Failed to add buffer to bufferQueue!");
            }
        }
    }

    public <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> visit) {
        createTopicIfNotExists(topic, false);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            try (Statement statement = transaction.connection().createStatement()) {
                statement.setFetchSize(10000);
                ResultSet rs = statement.executeQuery(String.format("SELECT key, value, ts FROM \"%s_bong_item\" ORDER BY key", topic));
                boolean next = rs.next(); // first
                while (next) {
                    byte[] key = rs.getBytes(1);
                    byte[] value = rs.getBytes(2);
                    K repositoryKey = RepositoryKey.fromByteBuffer(keyClass, ByteBuffer.wrap(key));
                    String content = new String(value, StandardCharsets.UTF_8);
                    visit.accept(Map.entry(repositoryKey, content), next = rs.next());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
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
