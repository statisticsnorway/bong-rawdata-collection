package no.ssb.dc.collection.api.worker;

public enum BackendProvider {

    NO_CACHE("no-cache"),
    LMDB("lmdb"),
    POSTGRES("postgres");

    public final String provider;

    BackendProvider(String provider) {
        this.provider = provider;
    }

    public static BackendProvider of(String provider) {
        for (BackendProvider value : values()) {
            if (value.provider.equalsIgnoreCase(provider)) {
                return value;
            }
        }
        throw new IllegalStateException("Enum value not supported: " + provider);
    }
}
