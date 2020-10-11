package no.ssb.dc.collection.api.worker;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator {

    private static final AtomicLong seq = new AtomicLong();

    public static Long next() {
        return seq.incrementAndGet();
    }
}
