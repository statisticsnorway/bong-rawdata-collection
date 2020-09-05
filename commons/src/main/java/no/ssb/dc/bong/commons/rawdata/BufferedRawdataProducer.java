package no.ssb.dc.bong.commons.rawdata;

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
    private final EncryptionClient encryptionClient;
    private final byte[] secretKey;

    public BufferedRawdataProducer(int queueBufferSize, RawdataProducer rawdataProducer) {
        this(queueBufferSize, rawdataProducer, null, null);
    }

    public BufferedRawdataProducer(int queueBufferSize, RawdataProducer rawdataProducer, final char[] encryptionKey, final byte[] encryptionSalt) {
        Objects.requireNonNull(rawdataProducer);
        this.rawdataProducer = rawdataProducer;
        threadPool = FixedThreadPool.newInstance();
        queue = new LinkedBlockingDeque<>(queueBufferSize);
        this.encryptionClient = new EncryptionClient();
        if (encryptionKey != null && encryptionKey.length > 0 && encryptionSalt != null && encryptionSalt.length > 0) {
            this.secretKey = encryptionClient.generateSecretKey(encryptionKey, encryptionSalt).getEncoded();
            Arrays.fill(encryptionKey, (char) 0);
            Arrays.fill(encryptionSalt, (byte) 0);
        } else {
            this.secretKey = null;
        }
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
