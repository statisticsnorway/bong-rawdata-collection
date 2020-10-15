package no.ssb.dc.collection.api.worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator {

    public enum Subject {
        DYNAMIC_KEY,
        POSITION_KEY;
    }

    private static final Map<Subject, AtomicLong> sequences = new ConcurrentHashMap<>();

    public static Long next(Subject subject) {
        return sequences.computeIfAbsent(subject, sequence -> new AtomicLong()).incrementAndGet();
    }
}
