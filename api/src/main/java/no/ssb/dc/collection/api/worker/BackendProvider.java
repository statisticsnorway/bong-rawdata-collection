package no.ssb.dc.collection.api.worker;

public enum BackendProvider {

    NO_CACHE("no-cache"),
    LMDB("lmdb"),
    POSTGRES("postgres");

    private final String name;

    BackendProvider(String name) {
        this.name = name;
    }

    public static BackendProvider of(String name) {
        for (BackendProvider value : values()) {
            if (value.name.equalsIgnoreCase(name)) {
                return value;
            }
        }
        throw new IllegalStateException("Enum value not supported: " + name);
    }
}
