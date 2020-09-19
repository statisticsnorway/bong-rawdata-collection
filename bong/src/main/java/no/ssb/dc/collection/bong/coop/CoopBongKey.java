package no.ssb.dc.collection.bong.coop;

import no.ssb.dc.collection.api.source.RepositoryKey;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CoopBongKey implements RepositoryKey {

    static final AtomicLong seq = new AtomicLong(0);

    final String filename;
    final Long locationNo;
    final Integer bongNo;
    final Long buyTimestamp;
    final Long lineIndex;

    public CoopBongKey() {
        this(null, null, null, null);
    }

    public CoopBongKey(String filename, Long locationNo, Integer bongNo, Long buyTimestamp) {
        this.filename = filename;
        this.locationNo = locationNo;
        this.bongNo = bongNo;
        this.buyTimestamp = buyTimestamp;
        this.lineIndex = seq.incrementAndGet();
    }

    public CoopBongKey(String filename, Long locationNo, Integer bongNo, Long buyTimestamp, Long lineIndex) {
        this.filename = filename;
        this.locationNo = locationNo;
        this.bongNo = bongNo;
        this.buyTimestamp = buyTimestamp;
        this.lineIndex = lineIndex;
    }

    @Override
    public RepositoryKey fromByteBuffer(ByteBuffer keyBuffer) {
        Objects.requireNonNull(keyBuffer);
        int filenameLength = keyBuffer.getInt();
        byte[] filenameBytes = new byte[filenameLength];
        keyBuffer.get(filenameBytes);
        long locationNo = keyBuffer.getLong();
        int bongNo = keyBuffer.getInt();
        long buyTimestamp = keyBuffer.getLong();
        long index = keyBuffer.getLong();
        return new CoopBongKey(new String(filenameBytes, StandardCharsets.UTF_8), locationNo, bongNo, buyTimestamp, index);
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
        Objects.requireNonNull(allocatedBuffer);
        byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
        allocatedBuffer.putInt(filenameBytes.length);
        allocatedBuffer.put(filenameBytes);
        allocatedBuffer.putLong(locationNo);
        allocatedBuffer.putInt(bongNo);
        allocatedBuffer.putLong(buyTimestamp);
        allocatedBuffer.putLong(lineIndex);
        return allocatedBuffer.flip();
    }

    @Override
    public String toPosition() {
        return String.format("%s.%s", locationNo, bongNo);
    }

    public boolean isPartOfBong(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoopBongKey NGBongKey = (CoopBongKey) o;
        return Objects.equals(locationNo, NGBongKey.locationNo) &&
                Objects.equals(bongNo, NGBongKey.bongNo);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoopBongKey NGBongKey = (CoopBongKey) o;
        return Objects.equals(locationNo, NGBongKey.locationNo) &&
                Objects.equals(bongNo, NGBongKey.bongNo) &&
                Objects.equals(buyTimestamp, NGBongKey.buyTimestamp) &&
                Objects.equals(lineIndex, NGBongKey.lineIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locationNo, bongNo, buyTimestamp, lineIndex);
    }

    @Override
    public String toString() {
        return "CoopBongKey{" +
                "locationNo=" + locationNo +
                ", bongNo=" + bongNo +
                ", buyTimestamp=" + buyTimestamp +
                ", index=" + lineIndex +
                '}';
    }
}
