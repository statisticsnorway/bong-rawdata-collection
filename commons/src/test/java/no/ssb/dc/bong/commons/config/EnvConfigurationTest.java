package no.ssb.dc.bong.commons.config;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EnvConfigurationTest {

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

    // --add-opens java.base/java.lang=bong.ng --add-opens java.base/java.util=bong.ng
    @Disabled
    @Test
    void thatSourceConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_source.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_source.csv.filepath", "filepath");
        getModifiableEnvironment().put("BONG_source.csv.files", "files");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, SourceLmdbConfiguration::new);

        // complete required keys
        getModifiableEnvironment().put("BONG_source.lmdb.path", "lmdbpath");

        // successful required keys config
        SourceLmdbConfiguration configuration = new SourceLmdbConfiguration();

        assertEquals("topic", configuration.asDynamicConfiguration().evaluateToString("rawdata.topic"));
        assertEquals("filepath", configuration.asDynamicConfiguration().evaluateToString("csv.filepath"));
        assertEquals("files", configuration.asDynamicConfiguration().evaluateToString("csv.files"));
        assertEquals("lmdbpath", configuration.asDynamicConfiguration().evaluateToString("lmdb.path"));
    }

    // --add-opens java.base/java.lang=bong.ng --add-opens java.base/java.util=bong.ng
    @Disabled
    @Test
    void thatTargetLocalFileSystemConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_target.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_target.local-temp-folder", "localTemp");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, LocalFileSystemConfiguration::new);

        // complete required keys
        getModifiableEnvironment().put("BONG_target.filesystem.storage-folder", "storageFolder");

        // successful required keys config
        LocalFileSystemConfiguration configuration = new LocalFileSystemConfiguration();

        assertEquals("localTemp", configuration.asDynamicConfiguration().evaluateToString("local-temp-folder"));
        assertEquals("storageFolder", configuration.asDynamicConfiguration().evaluateToString("filesystem.storage-folder"));
    }

    // --add-opens java.base/java.lang=bong.ng --add-opens java.base/java.util=bong.ng
    @Disabled
    @Test
    void thatTargetGCSConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_target.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_target.gcs.bucket-name", "bucket");
        getModifiableEnvironment().put("BONG_target.gcs.service-account.key-file", "gs_secret.json");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, GCSConfiguration::new);

        // complete required keys
        getModifiableEnvironment().put("BONG_target.local-temp-folder", "localTemp");

        // successful required keys config
        GCSConfiguration configuration = new GCSConfiguration();

        assertEquals("topic", configuration.asDynamicConfiguration().evaluateToString("rawdata.topic"));
        assertEquals("bucket", configuration.asDynamicConfiguration().evaluateToString("gcs.bucket-name"));
        assertEquals("gs_secret.json", configuration.asDynamicConfiguration().evaluateToString("gcs.service-account.key-file"));
        assertEquals("localTemp", configuration.asDynamicConfiguration().evaluateToString("local-temp-folder"));
    }
}
