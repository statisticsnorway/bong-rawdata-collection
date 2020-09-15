package no.ssb.dc.bong.rema;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.bong.commons.config.TargetConfiguration;
import no.ssb.dc.bong.commons.utils.FixedThreadPool;
import no.ssb.dc.bong.commons.utils.ULIDGenerator;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class RemaBongWorker implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RemaBongWorker.class);
    private static AtomicLong readFileCount = new AtomicLong();
    private static AtomicLong publishedMessageCount = new AtomicLong();
    private final DynamicConfiguration sourceConfiguration;
    private final DynamicConfiguration targetConfiguration;
    private final FixedThreadPool threadPool;
    private final BufferedReordering<String> bufferedReordering = new BufferedReordering<>();
    private final Queue<CompletableFuture<RawdataMessageBuffer>> futures;
    private final RawdataClient client;
    private final RawdataProducer producer;
    private final int queueCapacity;
    private final Path sourcePath;
    private final EncryptionClient encryptionClient;
    private final byte[] secretKey;

    public RemaBongWorker(SourceRemaConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        this.sourceConfiguration = sourceConfiguration.asDynamicConfiguration();
        this.targetConfiguration = targetConfiguration.asDynamicConfiguration();
        threadPool = FixedThreadPool.newInstance();
        client = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
        producer = client.producer(this.targetConfiguration.evaluateToString("rawdata.topic"));
        sourcePath = Paths.get(this.sourceConfiguration.evaluateToString("root.path"))
                .resolve(this.sourceConfiguration.evaluateToString("year"))
                .resolve(this.sourceConfiguration.evaluateToString("month"))
                .normalize()
                .toAbsolutePath();
        final char[] encryptionKey = this.targetConfiguration.evaluateToString("rawdata.encryptionKey") != null ?
                this.targetConfiguration.evaluateToString("rawdata.encryptionKey").toCharArray() : null;
        final byte[] encryptionSalt = this.targetConfiguration.evaluateToString("rawdata.encryptionSalt") != null ?
                this.targetConfiguration.evaluateToString("rawdata.encryptionSalt").getBytes() : null;
        this.encryptionClient = new EncryptionClient();
        if (encryptionKey != null && encryptionKey.length > 0 && encryptionSalt != null && encryptionSalt.length > 0) {
            this.secretKey = encryptionClient.generateSecretKey(encryptionKey, encryptionSalt).getEncoded();
            Arrays.fill(encryptionKey, (char) 0);
            Arrays.fill(encryptionSalt, (byte) 0);
        } else {
            this.secretKey = null;
        }
        queueCapacity = this.sourceConfiguration.evaluateToString("queue.capacity") != null ? this.sourceConfiguration.evaluateToInt("queue.capacity") : 1000;
        futures = new LinkedBlockingDeque<>(queueCapacity);
    }

    public boolean validate() {
        LOG.info("Validating source files: {}. Please wait for about a minute!", sourcePath.toString());
        if (!sourcePath.toFile().exists()) {
            LOG.error("Source folder not found: {}", sourcePath.toString());
        }
        AtomicLong countFiles = new AtomicLong();
        AtomicBoolean success = new AtomicBoolean(true);
        readFileTree((file, attrs) -> {
            RawdataMessageBuffer writer = new RawdataMessageBuffer(null, file);
            try {
                writer.toTimestamp();
            } catch (Exception e) {
                success.set(false);
                LOG.error("Failed to validate file [{}]: {}", countFiles.get(), writer.getFile().toString());
                return;
            }
            countFiles.incrementAndGet();
        });
        LOG.info("Validated {} files", countFiles.get());
        return success.get();
    }

    CompletableFuture<RawdataMessageBuffer> offerMessage(RawdataMessageBuffer message) {
        bufferedReordering.addExpected(message.toPosition());
        return CompletableFuture.supplyAsync(() -> {
                    message.produce();
                    return message;
                }, threadPool.getExecutor()
        ).thenApply(msg -> {
            bufferedReordering.addCompleted(msg.toPosition(), orderedPositions -> {
                String[] positions = orderedPositions.toArray(new String[0]);
                producer.publish(positions);
                publishedMessageCount.getAndAdd(positions.length);
            });
            return msg;
        }).exceptionally(throwable -> {
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new RuntimeException(throwable);
        });
    }

    void commitMessages() {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    futures.clear();
                    return v;
                })
                .join();
    }

    public void produce() {
        LOG.info("Source path: {}", sourcePath.toString());
        readFileTree((file, attrs) -> {
            if (readFileCount.incrementAndGet() % 10000 == 0) {
                LOG.info("Source - Read file count: {}", readFileCount.get());
                LOG.info("Source - Published message count: {}", publishedMessageCount.get());
            }
            CompletableFuture<RawdataMessageBuffer> future = offerMessage(new RawdataMessageBuffer(producer, file, encryptionClient, secretKey));
            if (!futures.offer(future)) {
                commitMessages();

                // re-offer message
                if (!futures.offer(future)) {
                    throw new IllegalStateException("Unable to offer future! Out of capacity: " + queueCapacity);
                }
            }
        });
    }

    void readFileTree(BiConsumer<Path, BasicFileAttributes> visitFile) {
        try {
            Files.walkFileTree(sourcePath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (file.getFileName().toString().startsWith(".")) {
                        return FileVisitResult.CONTINUE;
                    }
                    visitFile.accept(file, attrs);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            commitMessages();
            threadPool.shutdownAndAwaitTermination();
            client.close();
            LOG.info("Source - Read file Total-Count: {}", readFileCount.get());
            LOG.info("Source - Published message Total-Count: {}", publishedMessageCount.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class RawdataMessageBuffer {
        private final RawdataProducer producer;
        private final Path file;
        private final EncryptionClient encryptionClient;
        private final byte[] secretKey;

        public RawdataMessageBuffer(RawdataProducer producer, Path file) {
            this(producer, file, null, null);
        }

        public RawdataMessageBuffer(RawdataProducer producer, Path file, EncryptionClient encryptionClient, byte[] secretKey) {
            this.encryptionClient = encryptionClient;
            this.secretKey = secretKey;
            Objects.requireNonNull(file);
            this.producer = producer;
            this.file = file;
        }

        private byte[] tryEncryptContent(byte[] content) {
            if (secretKey != null) {
                byte[] iv = encryptionClient.generateIV();
                return encryptionClient.encrypt(secretKey, iv, content);
            }
            return content;
        }

        public void produce() {
            RawdataMessage.Builder messageBuilder = producer.builder();
            try {
                messageBuilder.position(toPosition());
                messageBuilder.ulid(ULIDGenerator.generate(toTimestamp()));
                messageBuilder.put("entry", tryEncryptContent(Files.readAllBytes(getFile())));
                producer.buffer(messageBuilder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Path getFile() {
            return file.normalize().toAbsolutePath();
        }

        public String getFilename() {
            return file.getFileName().toString();
        }

        public String toPosition() {
            return getFilename();
        }

        public Long toTimestamp() {
            int hyphenPos = toPosition().indexOf("-");
            if (hyphenPos == -1) {
                throw new IllegalStateException("Wrong filename format: \"" + toPosition() + "\"");
            }
            String timestampToken = toPosition().substring(0, hyphenPos).replace("_", "");
            return Long.valueOf(timestampToken);
        }
    }
}
