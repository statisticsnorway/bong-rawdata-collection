package no.ssb.dc.collection.kostra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.target.BufferedRawdataProducer;
import no.ssb.dc.collection.api.utils.FixedThreadPool;
import no.ssb.dc.collection.api.worker.JsonParser;
import no.ssb.dc.collection.api.worker.MetadataContent;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/*
    kostra stream

    1:
    key=manifest.json
        metadata + schema {mapping: art=string, periode=string}
    key=data
        {
            structure: [array],
            data: [[one-element]]
        }
    2:
        {
            structure: [array],
            data: [[one-element]]
        }

    dataset:

    header   art    periode
    1        10101  0104
    2        20101  8104
*/


public class KostraWorker implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KostraWorker.class);
    private static AtomicLong publishedMessageCount = new AtomicLong();
    private final SourceKostraConfiguration sourceConfiguration;
    private final TargetConfiguration targetConfiguration;
    private final FixedThreadPool threadPool;
    private final BufferedReordering<String> bufferedReordering = new BufferedReordering<>();
    private final Queue<CompletableFuture<RawdataMessageBuffer>> futures;
    private final RawdataClient client;
    private final RawdataProducer producer;
    private final int queueCapacity;
    private final Path sourcePath;
    private final JsonNode specification;
    private final EncryptionClient encryptionClient;
    private final byte[] secretKey;

    public KostraWorker(SourceKostraConfiguration sourceConfiguration, TargetConfiguration targetConfiguration) {
        this.sourceConfiguration = sourceConfiguration;
        this.targetConfiguration = targetConfiguration;
        threadPool = FixedThreadPool.newInstance();
        client = ProviderConfigurator.configure(targetConfiguration.asMap(), this.targetConfiguration.rawdataClientProvider(), RawdataClientInitializer.class);
        producer = client.producer(this.targetConfiguration.topic());
        sourcePath = Paths.get(this.sourceConfiguration.sourcePath());
        specification = loadSpecification(sourceConfiguration);
        final char[] encryptionKey = this.targetConfiguration.hasRawdataEncryptionKey() ?
                this.targetConfiguration.rawdataEncryptionKey().toCharArray() : null;
        final byte[] encryptionSalt = this.targetConfiguration.hasRawdataEncryptionSalt() ?
                this.targetConfiguration.rawdataEncryptionSalt().getBytes() : null;
        this.encryptionClient = new EncryptionClient();
        if (encryptionKey != null && encryptionKey.length > 0 && encryptionSalt != null && encryptionSalt.length > 0) {
            this.secretKey = encryptionClient.generateSecretKey(encryptionKey, encryptionSalt).getEncoded();
            Arrays.fill(encryptionKey, (char) 0);
            Arrays.fill(encryptionSalt, (byte) 0);
        } else {
            this.secretKey = null;
        }
        queueCapacity = this.sourceConfiguration.hasQueueCapacity() ? this.sourceConfiguration.queueCapacity() : 1000;
        futures = new LinkedBlockingDeque<>(queueCapacity);
    }

    private JsonNode loadSpecification(SourceKostraConfiguration sourceConfiguration) {
        Path specPath = Paths.get(sourceConfiguration.sourcePath()).resolve(Paths.get(sourceConfiguration.specificationFile()));
        try {
            byte[] yamlBytes = Files.readAllBytes(specPath);
            return JsonParser.createYamlParser().mapper().readValue(yamlBytes, JsonNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean validate() {
        // do nothing
        return true;
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

        Path jsonFilePath = sourcePath.resolve(Paths.get(sourceConfiguration.sourceFile()));

        try (InputStream is = new FileInputStream(jsonFilePath.toFile())) {
            JsonParser jsonParser = JsonParser.createJsonParser();

            JsonNode rootNode = jsonParser.fromJson(is, JsonNode.class);

            ArrayNode structureArrayNode = (ArrayNode) rootNode.get("structure");
            // TODO convert structure[].name and .type to schema mapping

            ArrayNode dataNode = (ArrayNode) rootNode.get("data");
            for (int i = 0; i < dataNode.size(); i++) {
                String position = String.valueOf(i + 1);
                ArrayNode dataElementNode = (ArrayNode) dataNode.get(i);

                // produce rawdata message
                ObjectNode targetElementDocument = jsonParser.createObjectNode();
                targetElementDocument.set("structure", structureArrayNode);
                ArrayNode targetDataArrayNode = jsonParser.createArrayNode();
                targetDataArrayNode.add(dataElementNode);
                targetElementDocument.set("data", targetDataArrayNode);
                byte[] bytes = jsonParser.toJSON(targetElementDocument).getBytes();
                LOG.trace("{}:\n{}", i + 1, jsonParser.toPrettyJSON(targetElementDocument));

                // produce manifest json
                JsonNode metadata = specification.withArray("metadata");
                JsonNode fileDescriptor = specification.withArray("fileDescriptor");
                MetadataContent.Builder metadataContentBuilder = new MetadataContent.Builder()
                        .topic(producer.topic())
                        .position(position)
                        .resourceType("entry")
                        .contentKey("entry")
                        .source(getString(metadata, "source"))
                        .dataset(getString(metadata, "dataset"))
                        .tag(getString(metadata, "tag"))
                        .description(getString(metadata, "description"))
                        .charset(StandardCharsets.UTF_8.displayName())
                        .contentType(getString(fileDescriptor, "contentType"))
                        .contentLength(bytes.length)
                        .markCreatedDate();

                // store json mapping
                metadataContentBuilder
                        .sourcePath(sourceConfiguration.sourcePath())
                        .sourceFile(sourceConfiguration.sourceFile())
                        .sourceCharset(getString(fileDescriptor, "charset"))
                        .recordType(BufferedRawdataProducer.RecordType.SINGLE.name().toLowerCase());

                for (int j = 0; j < structureArrayNode.size(); j++) {
                    JsonNode structureElementNode = structureArrayNode.get(j);
                    String name = structureElementNode.get("name").asText();
                    String type = structureElementNode.get("type").asText();
                    metadataContentBuilder.jsonMapping(name, asDataTypeFormat(type));
                }

                MetadataContent metadataContent = metadataContentBuilder.build();

                CompletableFuture<RawdataMessageBuffer> future = offerMessage(new RawdataMessageBuffer(producer, position, bytes, metadataContent, encryptionClient, secretKey));
                if (!futures.offer(future)) {
                    commitMessages();

                    // re-offer message
                    if (!futures.offer(future)) {
                        throw new IllegalStateException("Unable to offer future! Out of capacity: " + queueCapacity);
                    }
                }
            }

            //LOG.trace("{}", jsonParser.toPrettyJSON(root));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String getString(JsonNode jsonNode, String fieldName) {
        return jsonNode.findValue(fieldName) != null ? jsonNode.findValue(fieldName).asText() : null;
    }

    private String asDataTypeFormat(String type) {
        return String.format("%s%s", type.substring(0, 1).toUpperCase(), type.substring(1).toLowerCase());
    }


    @Override
    public void close() {
        try {
            commitMessages();
            threadPool.shutdownAndAwaitTermination();
            client.close();
            LOG.info("Source - Published message Total-Count: {}", publishedMessageCount.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class RawdataMessageBuffer {
        private final RawdataProducer producer;
        private final String position;
        private final byte[] data;
        private final EncryptionClient encryptionClient;
        private final byte[] secretKey;
        private final MetadataContent metadataContent;

        public RawdataMessageBuffer(RawdataProducer producer, String position, byte[] data, MetadataContent manifest) {
            this(producer, position, data, manifest, null, null);
        }

        public RawdataMessageBuffer(RawdataProducer producer, String position, byte[] data, MetadataContent metadataContent, EncryptionClient encryptionClient, byte[] secretKey) {
            this.metadataContent = metadataContent;
            Objects.requireNonNull(data);
            this.position = position;
            this.data = data;
            this.producer = producer;
            this.encryptionClient = encryptionClient;
            this.secretKey = secretKey;
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
            messageBuilder.position(toPosition());
            messageBuilder.put("manifest.json", JsonParser.createJsonParser().toJSON(metadataContent.getElementNode()).getBytes());
            messageBuilder.put("entry", tryEncryptContent(data));
            producer.buffer(messageBuilder);
        }

        public String toPosition() {
            return position;
        }
    }
}
