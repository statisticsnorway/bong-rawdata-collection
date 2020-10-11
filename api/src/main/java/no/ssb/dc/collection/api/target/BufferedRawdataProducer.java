package no.ssb.dc.collection.api.target;

import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.utils.FixedThreadPool;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

public class BufferedRawdataProducer implements AutoCloseable {

    static final Logger LOG = LoggerFactory.getLogger(BufferedRawdataProducer.class);

    final FixedThreadPool threadPool;
    final LinkedBlockingDeque<RawdataMessage> queue;
    final RawdataProducer rawdataProducer;
    final EncryptionClient encryptionClient;
    final byte[] secretKey;

    public BufferedRawdataProducer(TargetConfiguration targetConfiguration, int queueBufferSize, RawdataProducer rawdataProducer) {
        Objects.requireNonNull(rawdataProducer);
        this.rawdataProducer = rawdataProducer;
        threadPool = FixedThreadPool.newInstance();
        queue = new LinkedBlockingDeque<>(queueBufferSize);

        final char[] encryptionKey = targetConfiguration.hasRawdataEncryptionKey() ? targetConfiguration.rawdataEncryptionKey().toCharArray() : null;
        final byte[] encryptionSalt = targetConfiguration.hasRawdataEncryptionSalt() ? targetConfiguration.rawdataEncryptionSalt().getBytes() : null;

        this.encryptionClient = new EncryptionClient();

        if (encryptionKey != null && encryptionKey.length > 0 && encryptionSalt != null && encryptionSalt.length > 0) {
            this.secretKey = encryptionClient.generateSecretKey(encryptionKey, encryptionSalt).getEncoded();
            Arrays.fill(encryptionKey, (char) 0);
            Arrays.fill(encryptionSalt, (byte) 0);
        } else {
            this.secretKey = null;
        }
    }

    public RawdataProducer producer() {
        return rawdataProducer;
    }

    public void produce(RawdataMessage message) {
        if (!queue.offer(message)) {
            commitQueue();

            // try add violated capacity element
            if (!queue.add(message)) {
                throw new RuntimeException("Unable to queue message after publishing queue buffers");
            }
        }
    }

    void commitQueue() {
        List<CompletableFuture<String>> futureList = new ArrayList<>();
        List<String> publishPositionList = new ArrayList<>();
        RawdataMessage rawdataMessage;
        while ((rawdataMessage = queue.poll()) != null) {
            publishPositionList.add(rawdataMessage.position());
            AtomicReference<RawdataMessage> rawdataMessageRef = new AtomicReference<>(rawdataMessage);
            futureList.add(CompletableFuture.supplyAsync(() -> {
                RawdataMessage.Builder copyOfRawadataMessage = copyMessage(rawdataMessageRef.get());
                rawdataProducer.buffer(copyOfRawadataMessage);
                return copyOfRawadataMessage.position();
            }, threadPool.getExecutor()));
        }

        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    //LOG.info("Publish: {}", publishPositionList);
                    rawdataProducer.publish(publishPositionList.toArray(new String[0]));
                    return v;
                })
                .exceptionally(throwable -> {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    throw new RuntimeException(throwable);
                })
                .join();
    }

    RawdataMessage.Builder copyMessage(RawdataMessage sourceMessage) {
        RawdataMessage.Builder copyMessageBuilder = rawdataProducer.builder();
        copyMessageBuilder.position(sourceMessage.position());
        copyMessageBuilder.ulid(sourceMessage.ulid());
        for (String key : sourceMessage.keys()) {
            copyMessageBuilder.put(key, tryEncryptContent(sourceMessage.get(key)));
        }
        return copyMessageBuilder;
    }

    private byte[] tryEncryptContent(byte[] content) {
        if (secretKey != null) {
            byte[] iv = encryptionClient.generateIV();
            return encryptionClient.encrypt(secretKey, iv, content);
        }
        return content;
    }


    @Override
    public void close() {
        commitQueue();
        threadPool.shutdownAndAwaitTermination();
    }
}
