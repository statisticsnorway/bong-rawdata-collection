package no.ssb.dc.bong.commons.rawdata;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public interface RepositoryKey {

    static <R extends RepositoryKey> R fromByteBuffer(Class<R> keyClass, ByteBuffer keyBuffer) {
        try {
            R repositoryKey = keyClass.getDeclaredConstructor(new Class[0]).newInstance();
            return repositoryKey.fromByteBuffer(keyBuffer);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <R extends RepositoryKey> R fromByteBuffer(ByteBuffer keyBuffer);

    ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer);

    String toPosition();

}
