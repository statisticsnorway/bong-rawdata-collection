package no.ssb.dc.collection.client;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class IntegrationTest {

    static Map<String, String> getModifiableEnvironment() throws Exception {
        Class pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv");
        getenv.setAccessible(true);
        Object unmodifiableEnvironment = getenv.invoke(null);
        Class map = Class.forName("java.util.Collections$UnmodifiableMap");
        Field m = map.getDeclaredField("m");
        m.setAccessible(true);
        return (Map) m.get(unmodifiableEnvironment);
    }

    // vm-args:
    // --add-opens java.base/java.lang.invoke=rawdata.collection.client --add-opens java.base/java.util=rawdata.collection.client --add-opens java.base/java.lang=rawdata.collection.client --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED
    @Disabled
    @Test
    void application() throws Exception {
        Path currentPath = Paths.get(".").normalize().toAbsolutePath();
        Path resourcePath = currentPath.getParent().resolve(Paths.get("api/src/test/resources/no/ssb/dc/collection/api/worker"));
        getModifiableEnvironment().put("BONG_action", "generate");
        getModifiableEnvironment().put("BONG_target", "dynamic-lmdb");
        getModifiableEnvironment().put("BONG_source.specification.filepath", resourcePath.toString());
        getModifiableEnvironment().put("BONG_source.specification.file", "generic-spec.yaml");
        getModifiableEnvironment().put("BONG_source.lmdb.path", currentPath.resolve(Paths.get("target/lmdb")).toString());
        getModifiableEnvironment().put("BONG_source.rawdata.topic", "source-topic");
        getModifiableEnvironment().put("BONG_source.csv.filepath", resourcePath.toString());
        getModifiableEnvironment().put("BONG_source.csv.files", "");
        getModifiableEnvironment().put("BONG_target.rawdata.client.provider", "filesystem");
        getModifiableEnvironment().put("BONG_target.rawdata.topic", "target-topic");
        getModifiableEnvironment().put("BONG_target.local-temp-folder", currentPath.resolve(Paths.get("target/temp")).toString());
        getModifiableEnvironment().put("BONG_target.filesystem.storage-folder", currentPath.resolve(Paths.get("target/rawdata")).toString());

        Application app = Application.create();
        app.run();
    }
}
