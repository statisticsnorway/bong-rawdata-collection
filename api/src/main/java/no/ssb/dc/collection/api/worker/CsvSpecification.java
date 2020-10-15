package no.ssb.dc.collection.api.worker;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;
import static java.util.stream.Collectors.toMap;

@JsonInclude(Include.NON_NULL)
public class CsvSpecification {

    public final BackendProvider backend;
    public final Metadata metadata;
    public final FileDescriptor fileDescriptor;
    public final ColumnKeys columns;

    public CsvSpecification(BackendProvider backend,
                            Metadata metadata,
                            FileDescriptor fileDescriptor,
                            ColumnKeys columns) {
        this.backend = backend;
        this.metadata = metadata;
        this.fileDescriptor = fileDescriptor;
        this.columns = columns;
    }

    public static Metadata.Builder metadata() {
        return new Metadata.Builder();
    }

    public static FileDescriptor.Builder fileDescriptor() {
        return new FileDescriptor.Builder();
    }

    public static ColumnKey.Builder column() {
        return new ColumnKey.Builder();
    }

    public static FunctionKey.Builder function() {
        return new FunctionKey.Builder();
    }

    @Override
    public String toString() {
        return "CsvSpecification{" +
                "backend='" + backend + '\'' +
                ", metadata=" + metadata +
                ", fileDescriptor=" + fileDescriptor +
                ", columns=" + columns +
                '}';
    }

    public static class Builder {
        private final Map<String, Object> fieldMap = new LinkedHashMap<>();
        private Metadata.Builder metadataBuilder;
        private FileDescriptor.Builder fileDescriptorBuilder;
        private ColumnKeys.Builder keysBuilder = new ColumnKeys.Builder();

        public Builder backend(BackendProvider backend) {
            fieldMap.put("backend", backend);
            return this;
        }

        public Builder metadata(Metadata.Builder metadataBuilder) {
            this.metadataBuilder = metadataBuilder;
            return this;
        }

        public Builder fileDescriptor(FileDescriptor.Builder fileDescriptorBuilder) {
            this.fileDescriptorBuilder = fileDescriptorBuilder;
            return this;
        }

        public <K extends Key.Builder<?, ?>> Builder columnKeys(K keysBuilder) {
            this.keysBuilder.key(keysBuilder);
            return this;
        }

        public CsvSpecification build() {
            if (metadataBuilder == null) {
                throw new IllegalStateException("No metadata definition defined!");
            }

            if (fileDescriptorBuilder == null) {
                throw new IllegalStateException("No fileDescriptor definition defined!");
            }

            if (keysBuilder.keyBuilderMap.isEmpty()) {
                throw new IllegalStateException("No column key definition defined!");
            }

            return new CsvSpecification(
                    (BackendProvider) fieldMap.get("backend"),
                    metadataBuilder.build(),
                    fileDescriptorBuilder.build(),
                    keysBuilder.build()
            );
        }
    }

    @JsonInclude(Include.NON_NULL)
    public static class Metadata {

        public final String source;
        public final String dataset;
        public final String tag;
        public final String description;

        public Metadata(String source, String dataset, String tag, String description) {
            this.source = source;
            this.dataset = dataset;
            this.tag = tag;
            this.description = description;
        }

        @Override
        public String toString() {
            return "Metadata{" +
                    "source='" + source + '\'' +
                    ", dataset='" + dataset + '\'' +
                    ", tag='" + tag + '\'' +
                    ", description='" + description + '\'' +
                    '}';
        }

        public static class Builder {
            private final Map<String, String> map = new LinkedHashMap<>();

            public Builder source(String source) {
                map.put("source", source);
                return this;
            }

            public Builder dataset(String dataset) {
                map.put("dataset", dataset);
                return this;
            }

            public Builder tag(String tag) {
                map.put("tag", tag);
                return this;
            }

            public Builder description(String description) {
                map.put("description", description);
                return this;
            }

            public Metadata build() {
                return new Metadata(
                        map.get("source"),
                        map.get("dataset"),
                        map.get("tag"),
                        map.get("description")
                );
            }

            public MetadataContent.Builder contentType(String displayName) {
                return null;
            }
        }
    }

    @JsonInclude(Include.NON_NULL)
    public static class FileDescriptor {

        public final char delimiter;
        public final Charset charset;
        public final String contentType;
        public final String files;

        public FileDescriptor(char delimiter, Charset charset, String contentType, String files) {
            this.delimiter = delimiter;
            this.charset = charset;
            this.contentType = contentType;
            this.files = files;
        }

        @Override
        public String toString() {
            return "FileDescriptor{" +
                    "delimiter=" + delimiter +
                    ", charset=" + charset +
                    ", contentType='" + contentType + '\'' +
                    ", files='" + files + '\'' +
                    '}';
        }

        public static class Builder {
            private final Map<String, Object> map = new LinkedHashMap<>();

            public Builder delimiter(char delimiter) {
                map.put("delimiter", delimiter);
                return this;
            }

            public Builder charset(Charset charset) {
                map.put("charset", charset);
                return this;
            }

            public Builder contentType(String contentType) {
                map.put("contentType", contentType);
                return this;
            }

            public Builder files(String files) {
                map.put("files", files);
                return this;
            }

            public FileDescriptor build() {
                return new FileDescriptor(
                        (char) map.get("delimiter"),
                        (Charset) map.get("charset"),
                        (String) map.get("contentType"),
                        (String) map.get("files")
                );
            }
        }
    }

    @JsonInclude(Include.NON_NULL)
    public static class ColumnKeys {

        private final Map<String, Key> keyMap;

        public ColumnKeys(Map<String, Key> keyMap) {
            this.keyMap = keyMap;
        }

        public ColumnKey columnKey(String name) {
            Key key = keyMap.get(name);
            if (!key.isColumn()) {
                throw new IllegalStateException();
            }
            return key.asColumn();
        }

        public FunctionKey functionKey(String name) {
            Key key = keyMap.get(name);
            if (!key.isFunction()) {
                throw new IllegalStateException();
            }
            return key.asFunction();
        }

        /**
         * KeyValue store key = keys(). If this is too granular, then expand with a custom marker.
         */
        @JsonGetter
        public Map<String, Key> keys() {
            return keyMap;
        }

        /**
         * Rawdata position = positionKeys()
         */
        public Map<String, Key> positionKeys() {
            return keyMap.entrySet().stream().filter(entry -> entry.getValue().isPosition()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        /**
         * Grouping of rawdata message content = groupByKeys()
         */
        public Map<String, Key> groupByKeys() {
            return keyMap.entrySet().stream().filter(entry -> entry.getValue().isGroupBy()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnKeys that = (ColumnKeys) o;
            return Objects.equals(keyMap, that.keyMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyMap);
        }

        @Override
        public String toString() {
            return "ColumnKeys{" +
                    "keyMap=" + keyMap +
                    '}';
        }

        public static class Builder {
            final Map<String, Key.Builder<?, ?>> keyBuilderMap = new LinkedHashMap<>();

            public <K extends Key.Builder<?, ?>> Builder key(K keyBuilder) {
                keyBuilderMap.put(keyBuilder.name(), keyBuilder);
                return this;
            }

            public ColumnKeys build() {
                Map<String, Key> keyMap = keyBuilderMap.entrySet().stream().collect(toMap(Map.Entry::getKey, value -> (Key) value.getValue().build(), (e1, e2) -> e1, LinkedHashMap::new));
                return new ColumnKeys(keyMap);
            }
        }
    }

    @JsonInclude(Include.NON_NULL)
    abstract public static class Key {

        public final String name;
        public final Class<?> type;
        private final boolean partOfPosition;
        private final boolean partOfGroupBy;

        public Key(String name, Class<?> type, boolean partOfPosition, boolean partOfGroupBy) {
            this.name = name;
            this.type = type;
            this.partOfPosition = partOfPosition;
            this.partOfGroupBy = partOfGroupBy;
        }

        @JsonGetter
        public String kind() {
            return isColumn() ? "column" : isFunction() ? "function" : unsupported();
        }

        private String unsupported() {
            throw new UnsupportedOperationException();
        }

        @JsonIgnore
        public boolean isColumn() {
            return this instanceof ColumnKey;
        }

        public ColumnKey asColumn() {
            return (ColumnKey) this;
        }

        @JsonIgnore
        public boolean isFunction() {
            return this instanceof FunctionKey;
        }

        public FunctionKey asFunction() {
            return (FunctionKey) this;
        }

        public boolean isPosition() {
            return partOfPosition;
        }

        public boolean isGroupBy() {
            return partOfGroupBy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return partOfPosition == key.partOfPosition &&
                    partOfGroupBy == key.partOfGroupBy &&
                    Objects.equals(name, key.name) &&
                    Objects.equals(type, key.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, partOfPosition, partOfGroupBy);
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

            public BUILDER groupBy() {
                map.put("groupBy", true);
                return (BUILDER) this;
            }

            abstract KEY build();
        }
    }

    @JsonInclude(Include.NON_NULL)
    public static class ColumnKey extends Key {
        public final String format;

        public ColumnKey(String name, Class<?> type, String format, boolean partOfPosition, boolean partOfGroupBy) {
            super(name, type, partOfPosition, partOfGroupBy);
            this.format = format;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ColumnKey columnKey = (ColumnKey) o;
            return Objects.equals(format, columnKey.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), type, format);
        }

        @Override
        public String toString() {
            return "ColumnKey{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", format='" + format + '\'' +
                    ", partOfPosition=" + isPosition() +
                    ", partOfGroupBy=" + isGroupBy() +
                    '}';
        }

        public static class Builder extends Key.Builder<ColumnKey, ColumnKey.Builder> {

            public Builder format(String format) {
                map.put("format", format);
                return this;
            }

            @Override
            ColumnKey build() {
                if (!map.containsKey("name")) {
                    throw new IllegalStateException("Collum name IS NOT defined!");
                }
                if (!map.containsKey("type")) {
                    throw new IllegalStateException("Collum type IS NOT defined!");
                }
                return new ColumnKey(
                        (String) map.get("name"),
                        (Class<?>) map.get("type"),
                        (String) map.get("format"),
                        map.containsKey("position") && Boolean.parseBoolean(String.valueOf(map.get("position"))),
                        map.containsKey("groupBy") && Boolean.parseBoolean(String.valueOf(map.get("groupBy")))
                );
            }
        }
    }


    @JsonInclude(Include.NON_NULL)
    public static class FunctionKey extends Key {

        public final KeyGenerator generator;

        public FunctionKey(String name, Class<?> type, KeyGenerator generator, boolean partOfPosition, boolean partOfGroupBy) {
            super(name, type, partOfPosition, partOfGroupBy);
            this.generator = generator;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            FunctionKey that = (FunctionKey) o;
            return generator == that.generator;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), generator);
        }

        @Override
        public String toString() {
            return "FunctionKey{" +
                    "name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", generator=" + generator +
                    ", partOfPosition=" + isPosition() +
                    ", partOfGroupBy=" + isGroupBy() +
                    '}';
        }

        public static class Builder extends Key.Builder<FunctionKey, FunctionKey.Builder> {

            public Builder generator(KeyGenerator generator) {
                map.put("generator", generator);
                map.put("type", generator.type);
                return this;
            }

            @Override
            FunctionKey build() {
                return new FunctionKey(
                        (String) map.get("name"),
                        (Class<?>) map.get("type"),
                        (KeyGenerator) map.get("generator"),
                        map.containsKey("position"),
                        map.containsKey("groupBy")
                );
            }
        }
    }

}
