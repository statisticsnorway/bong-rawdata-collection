package no.ssb.dc.collection.api.worker;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AbstractSpecification {

    @JsonInclude(JsonInclude.Include.NON_NULL)
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

}
