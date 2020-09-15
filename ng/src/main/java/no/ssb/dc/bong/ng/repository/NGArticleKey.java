package no.ssb.dc.bong.ng.repository;

import no.ssb.dc.bong.commons.source.RepositoryKey;

import java.nio.ByteBuffer;
import java.util.Objects;

public class NGArticleKey implements RepositoryKey {

    final String filename;
    final Long articleNo;

    public NGArticleKey(String filename, Long articleNo) {
        this.filename = filename;
        this.articleNo = articleNo;
    }

    @Override
    public RepositoryKey fromByteBuffer(ByteBuffer keyBuffer) {
        return null;
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
        return null;
    }

    @Override
    public String toPosition() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NGArticleKey that = (NGArticleKey) o;
        return Objects.equals(filename, that.filename) &&
                Objects.equals(articleNo, that.articleNo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, articleNo);
    }

    @Override
    public String toString() {
        return "NGArticleKey{" +
                "filename='" + filename + '\'' +
                ", articleNo=" + articleNo +
                '}';
    }
}
