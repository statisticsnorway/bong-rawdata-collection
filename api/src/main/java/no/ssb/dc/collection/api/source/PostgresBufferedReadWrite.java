package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.jdbc.PostgresDataSource;
import no.ssb.dc.collection.api.jdbc.PostgresTransaction;
import no.ssb.dc.collection.api.jdbc.PostgresTransactionFactory;
import no.ssb.dc.collection.api.utils.DirectByteBufferPool;
import no.ssb.dc.collection.api.utils.FixedThreadPool;
import no.ssb.dc.collection.api.worker.CsvSpecification;

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

public class PostgresBufferedReadWrite implements BufferedReadWrite {

    final FixedThreadPool threadPool;
    final Queue<Map.Entry<ByteBuffer, ByteBuffer>> bufferQueue;
    final DirectByteBufferPool keyPool;
    final DirectByteBufferPool valuePool;
    final PostgresTransactionFactory transactionFactory;
    final CsvSpecification specification;
    final String topic;
    final AtomicBoolean isTableCreated = new AtomicBoolean(false);
    final AtomicBoolean closed = new AtomicBoolean(false);

    public PostgresBufferedReadWrite(SourcePostgresConfiguration configuration, PostgresTransactionFactory transactionFactory, CsvSpecification specification) {
        this.transactionFactory = transactionFactory;
        this.specification = specification;
        this.threadPool = FixedThreadPool.newInstance();
        this.topic = configuration.topic();
        this.bufferQueue = new LinkedBlockingDeque<>(configuration.queuePoolSize());
        this.keyPool = new DirectByteBufferPool(configuration.queuePoolSize() + 1, configuration.queueKeyBufferSize());
        this.valuePool = new DirectByteBufferPool(configuration.queuePoolSize() + 1, configuration.queueValueBufferSize());
    }

    void createTopicIfNotExists(String topic, boolean dropAndCreateDatabase) {
        if (!isTableCreated.get() && (!transactionFactory.checkIfTableTopicExists(topic, "meta_item") || dropAndCreateDatabase)) {
            PostgresDataSource.dropOrCreateDatabase(transactionFactory, topic);
            isTableCreated.set(true);
        } else if (!dropAndCreateDatabase) {
            isTableCreated.set(true);
        }
    }

    @Override
    public void commitQueue() {
        createTopicIfNotExists(topic, false);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            Map.Entry<ByteBuffer, ByteBuffer> buffer;
            try (PreparedStatement ps = transaction.connection().prepareStatement(String.format("INSERT INTO \"%s_record_item\" (key, value, ts) VALUES (?, ?, ?)", topic))) {
                while ((buffer = bufferQueue.poll()) != null) {
                    try {
                        byte[] key = new byte[buffer.getKey().remaining()];
                        byte[] value = new byte[buffer.getValue().remaining()];
                        buffer.getKey().get(key);
                        buffer.getValue().get(value);
                        ps.setBytes(1, key);  // sortable byte buffer
                        ps.setBytes(2, value);
                        ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                        ps.addBatch();

                    } finally {
                        keyPool.release(buffer.getKey());
                        valuePool.release(buffer.getValue());
                    }
                }
                ps.executeBatch();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeHeader(String key, String value) {
        if (closed.get()) {
            throw new RuntimeException("Buffer is closed!");
        }
        createTopicIfNotExists(topic, true);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            String sql = String.format("INSERT INTO \"%s_meta_item\" (key, value, ts) VALUES (?, ?, ?)", topic);
            try (PreparedStatement ps = transaction.connection().prepareStatement(sql)) {
                byte[] dbKey = key.getBytes();
                byte[] dbValue = value.getBytes();
                ps.setBytes(1, dbKey);
                ps.setBytes(2, dbValue);
                ps.setTimestamp(3, Timestamp.from(new Date().toInstant()));
                ps.addBatch();
                ps.executeUpdate();

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public <K extends RepositoryKey> void writeRecord(K key, String value) {
        if (closed.get()) {
            throw new RuntimeException("Buffer is closed!");
        }
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

    @Override
    public void readHeader(BiConsumer<Map.Entry<String, String>, Boolean> handleRecordCallback) {
        if (closed.get()) {
            throw new RuntimeException("Buffer is closed!");
        }
        createTopicIfNotExists(topic, false);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            try (Statement statement = transaction.connection().createStatement()) {
                statement.setFetchSize(10000);
                ResultSet rs = statement.executeQuery(String.format("SELECT key, value, ts FROM \"%s_%s\" ORDER BY key", topic, "meta_item"));
                boolean next = rs.next(); // first
                while (next) {
                    byte[] dbKey = rs.getBytes(1);
                    String key = new String(dbKey);

                    byte[] dbValue = rs.getBytes(2);
                    String value = new String(dbValue);

                    handleRecordCallback.accept(Map.entry(key, value), next = rs.next()); // next
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public <K extends RepositoryKey> void readRecord(Class<K> keyClass, BiConsumer<Map.Entry<K, String>, Boolean> handleRecordCallback) {
        if (closed.get()) {
            throw new RuntimeException("Buffer is closed!");
        }
        createTopicIfNotExists(topic, false);
        try (PostgresTransaction transaction = transactionFactory.createTransaction(false)) {
            try (Statement statement = transaction.connection().createStatement()) {
                statement.setFetchSize(10000);
                ResultSet rs = statement.executeQuery(String.format("SELECT key, value, ts FROM \"%s_%s\" ORDER BY key", topic, "record_item"));
                boolean next = rs.next(); // first
                while (next) {
                    byte[] key = rs.getBytes(1);
                    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
                    K repositoryKey = RepositoryKey.fromByteBuffer(keyClass, keyBuffer, specification);

                    byte[] value = rs.getBytes(2);
                    ByteBuffer valueBuffer = ByteBuffer.wrap(value);
                    int valueByteLength = valueBuffer.getInt();
                    byte[] valueBytes = new byte[valueByteLength];
                    valueBuffer.get(valueBytes);
                    String content = new String(valueBytes, StandardCharsets.UTF_8);

                    handleRecordCallback.accept(Map.entry(repositoryKey, content), next = rs.next()); // next
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            commitQueue();
            keyPool.close();
            valuePool.close();
        }
    }

}
