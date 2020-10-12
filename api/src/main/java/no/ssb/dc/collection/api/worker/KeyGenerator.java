package no.ssb.dc.collection.api.worker;

public enum KeyGenerator {
    SEQUENCE(Long.class),
    ULID(String.class),
    UUID(String.class);

    public final Class<?> type;

    KeyGenerator(Class<?> type) {
        this.type = type;
    }

}
