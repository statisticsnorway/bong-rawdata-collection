package no.ssb.dc.collection.api.worker;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JsonSpecification extends AbstractSpecification {

    public final Metadata metadata;
    public final FileDescriptor fileDescriptor;
    public final Identifiers identifiers;

    public JsonSpecification(Metadata metadata,
                             FileDescriptor fileDescriptor,
                             Identifiers identifiers) {
        this.metadata = metadata;
        this.fileDescriptor = fileDescriptor;
        this.identifiers = identifiers;
    }

    public static Metadata.Builder metadata() {
        return new Metadata.Builder();
    }

    public static FileDescriptor.Builder fileDescriptor() {
        return new FileDescriptor.Builder();
    }

    public static Sequence.Builder sequence() {
        return new Sequence.Builder();
    }

    public static Function.Builder function() {
        return new Function.Builder();
    }

    @Override
    public String toString() {
        return "JsonSpecification{" +
                "metadata=" + metadata +
                ", fileDescriptor=" + fileDescriptor +
                ", columns=" + identifiers +
                '}';
    }

    public static class Builder {
        private final Map<String, Object> fieldMap = new LinkedHashMap<>();
        private Metadata.Builder metadataBuilder;
        private FileDescriptor.Builder fileDescriptorBuilder;
        private Identifiers.Builder identifiersBuilder = new Identifiers.Builder();

        public Builder metadata(Metadata.Builder metadataBuilder) {
            this.metadataBuilder = metadataBuilder;
            return this;
        }

        public Builder fileDescriptor(FileDescriptor.Builder fileDescriptorBuilder) {
            this.fileDescriptorBuilder = fileDescriptorBuilder;
            return this;
        }

        public <K extends Identifier.Builder<?, ?>> Builder identifiers(K keysBuilder) {
            this.identifiersBuilder.key(keysBuilder);
            return this;
        }

        public JsonSpecification build() {
            if (metadataBuilder == null) {
                throw new IllegalStateException("No metadata definition defined!");
            }

            if (fileDescriptorBuilder == null) {
                throw new IllegalStateException("No fileDescriptor definition defined!");
            }

            if (identifiersBuilder.keyBuilderMap.isEmpty()) {
                throw new IllegalStateException("No column key definition defined!");
            }

            return new JsonSpecification(
                    metadataBuilder.build(),
                    fileDescriptorBuilder.build(),
                    identifiersBuilder.build()
            );
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FileDescriptor {

        public final Charset charset;
        public final String contentType;
        public final String files;

        public FileDescriptor(Charset charset, String contentType, String files) {
            this.charset = charset;
            this.contentType = contentType;
            this.files = files;
        }

        @Override
        public String toString() {
            return "FileDescriptor{" +
                    "charset=" + charset +
                    ", contentType='" + contentType + '\'' +
                    ", files='" + files + '\'' +
                    '}';
        }

        public static class Builder {
            private final Map<String, Object> map = new LinkedHashMap<>();

            public FileDescriptor.Builder charset(Charset charset) {
                map.put("charset", charset);
                return this;
            }

            public FileDescriptor.Builder contentType(String contentType) {
                map.put("contentType", contentType);
                return this;
            }

            public FileDescriptor.Builder files(String files) {
                map.put("files", files);
                return this;
            }

            public FileDescriptor build() {
                return new FileDescriptor(
                        (Charset) map.get("charset"),
                        (String) map.get("contentType"),
                        (String) map.get("files")
                );
            }
        }
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Identifiers {

        private final Map<String, Identifier> identifierMap;

        public Identifiers(Map<String, Identifier> identifierMap) {
            this.identifierMap = identifierMap;
        }

        @JsonGetter
        public Sequence sequence(String name) {
            Identifier identifier = identifierMap.get(name);
            if (!identifier.isSequence()) {
                throw new IllegalStateException();
            }
            return identifier.asSequence();
        }

        public Function function(String name) {
            Identifier identifier = identifierMap.get(name);
            if (!identifier.isFunction()) {
                throw new IllegalStateException();
            }
            return identifier.asFunction();
        }

        /**
         * KeyValue store key/value map = array()
         */
        @JsonGetter
        public Map<String, Identifier> array() {
            return identifierMap;
        }

        /**
         * Rawdata position = positionKeys()
         */
        public Map<String, Identifier> positionKeys() {
            return identifierMap.entrySet().stream().filter(entry -> entry.getValue().isPosition()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifiers that = (Identifiers) o;
            return Objects.equals(identifierMap, that.identifierMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identifierMap);
        }

        @Override
        public String toString() {
            return "Identifiers{" +
                    "identifierMap=" + identifierMap +
                    '}';
        }

        public static class Builder {
            final Map<String, Identifier.Builder<?, ?>> keyBuilderMap = new LinkedHashMap<>();

            public <K extends Identifier.Builder<?, ?>> Identifiers.Builder key(K keyBuilder) {
                keyBuilderMap.put(keyBuilder.name(), keyBuilder);
                return this;
            }

            public Identifiers build() {
                Map<String, Identifier> identifierMap = keyBuilderMap.entrySet().stream().collect(toMap(Map.Entry::getKey, value -> (Identifier) value.getValue().build(), (e1, e2) -> e1, LinkedHashMap::new));
                return new Identifiers(identifierMap);
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    abstract public static class Identifier {

        public final String name;
        public final Class<?> type;
        protected final boolean partOfPosition;

        public Identifier(String name, Class<?> type, boolean partOfPosition) {
            this.name = name;
            this.type = type;
            this.partOfPosition = partOfPosition;
        }

        @JsonGetter
        public String kind() {
            return isSequence() ? "sequence" : isFunction() ? "function" : unsupported();
        }

        private String unsupported() {
            throw new UnsupportedOperationException();
        }

        @JsonIgnore
        public boolean isSequence() {
            return this instanceof Sequence;
        }

        public Sequence asSequence() {
            return (Sequence) this;
        }

        @JsonIgnore
        public boolean isFunction() {
            return this instanceof Function;
        }

        public Function asFunction() {
            return (Function) this;
        }

        public boolean isPosition() {
            return partOfPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifier identifier = (Identifier) o;
            return partOfPosition == identifier.partOfPosition &&
                    Objects.equals(name, identifier.name) &&
                    Objects.equals(type, identifier.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, partOfPosition);
        }

        abstract public static class Builder<KEY, BUILDER> {
            protected final Map<String, Object> map = new LinkedHashMap<>();

            public BUILDER name(String name) {
                map.put("name", name);
                return (BUILDER) this;
            }

            String name() {
                return (String) map.get("name");
            }

            public BUILDER type(Class<?> type) {
                map.put("type", type);
                return (BUILDER) this;
            }

            public BUILDER position() {
                map.put("position", true);
                return (BUILDER) this;
            }

            abstract KEY build();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Sequence extends Identifier {

        public Sequence(String name, Class<?> type, boolean partOfPosition) {
            super(name, type, partOfPosition);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public String toString() {
            return "Sequence{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", partOfPosition=" + partOfPosition +
                    '}';
        }

        public static class Builder extends Identifier.Builder<Sequence, Sequence.Builder> {

            @Override
            Sequence build() {
                if (!map.containsKey("name")) {
                    throw new IllegalStateException("Sequence name IS NOT defined!");
                }
                return new Sequence(
                        (String) map.get("name"),
                        (Class<?>) map.get("type"),
                        map.containsKey("position") && Boolean.parseBoolean(String.valueOf(map.get("position")))
                );
            }
        }
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Function extends Identifier {

        public final KeyGenerator generator;

        public Function(String name, Class<?> type, KeyGenerator generator, boolean partOfPosition) {
            super(name, type, partOfPosition);
            this.generator = generator;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Function that = (Function) o;
            return generator == that.generator;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), generator);
        }

        @Override
        public String toString() {
            return "Function{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", generator=" + generator +
                    ", partOfPosition=" + isPosition() +
                    '}';
        }

        public static class Builder extends Identifier.Builder<Function, Function.Builder> {

            public Function.Builder generator(KeyGenerator generator) {
                map.put("generator", generator);
                map.put("type", generator.type);
                return this;
            }

            @Override
            Function build() {
                return new Function(
                        (String) map.get("name"),
                        (Class<?>) map.get("type"),
                        (KeyGenerator) map.get("generator"),
                        map.containsKey("position")
                );
            }
        }
    }
}
