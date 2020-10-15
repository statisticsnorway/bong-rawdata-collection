package no.ssb.dc.collection.api.source;

import no.ssb.dc.collection.api.worker.CsvSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public interface RepositoryKey {

    Logger LOG = LoggerFactory.getLogger(RepositoryKey.class);

    static <R extends RepositoryKey> R fromByteBuffer(Class<R> keyClass, ByteBuffer keyBuffer) {
        return fromByteBuffer(keyClass, keyBuffer, null);
    }

    static <R extends RepositoryKey> R fromByteBuffer(Class<R> keyClass, ByteBuffer keyBuffer, CsvSpecification specification) {
        try {
            R repositoryKey = specification == null ?
                    keyClass.getDeclaredConstructor(new Class[0]).newInstance() :
                    keyClass.getDeclaredConstructor(CsvSpecification.class).newInstance(specification);

            return specification == null ?
                    repositoryKey.fromByteBuffer(keyBuffer) :
                    repositoryKey.fromByteBuffer(specification, keyBuffer);

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            LOG.error("CsvSpecification is: {}", specification);
            throw new RuntimeException(e);
        }
    }

    <R extends RepositoryKey> R fromByteBuffer(CsvSpecification specification, ByteBuffer keyBuffer);

    <R extends RepositoryKey> R fromByteBuffer(ByteBuffer keyBuffer);

    void toByteBuffer(ByteBuffer allocatedBuffer);

    String toPosition();

}
