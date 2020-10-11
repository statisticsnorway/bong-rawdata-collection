package no.ssb.dc.collection.api.config;

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

    // --add-opens java.base/java.lang=bong.commons --add-opens java.base/java.util=bong.commons
    @Disabled
    @Test
    void thatSourceConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_source.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_source.csv.filepath", "filepath");
        getModifiableEnvironment().put("BONG_source.csv.files", "files");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, SourceLmdbConfiguration::create);

        // complete required keys
        getModifiableEnvironment().put("BONG_source.lmdb.path", "lmdbpath");

        // successful required keys config
        SourceLmdbConfiguration configuration = SourceLmdbConfiguration.create();

        assertEquals("topic", configuration.topic());
        assertEquals("filepath", configuration.filePath());
        assertEquals("files", configuration.csvFiles());
        assertEquals("lmdbpath", configuration.lmdbPath());
    }

    // --add-opens java.base/java.lang=bong.commons --add-opens java.base/java.util=bong.commons
    @Disabled
    @Test
    void thatTargetLocalFileSystemConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_target.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_target.local-temp-folder", "localTemp");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, LocalFileSystemConfiguration::create);

        // complete required keys
        getModifiableEnvironment().put("BONG_target.filesystem.storage-folder", "storageFolder");

        // successful required keys config
        LocalFileSystemConfiguration configuration = LocalFileSystemConfiguration.create();

        assertEquals("localTemp", configuration.localTempFolder());
        assertEquals("storageFolder", configuration.localStorageFolder());
    }

    // --add-opens java.base/java.lang=bong.commons --add-opens java.base/java.util=bong.commons
    @Disabled
    @Test
    void thatTargetGCSConfigurationReadsPrefixOverride() throws Exception {
        getModifiableEnvironment().put("BONG_target.rawdata.topic", "topic");
        getModifiableEnvironment().put("BONG_target.gcs.bucket-name", "bucket");
        getModifiableEnvironment().put("BONG_target.gcs.service-account.key-file", "gs_secret.json");

        // fail on missing required keys
        assertThrows(IllegalArgumentException.class, GCSConfiguration::create);

        // complete required keys
        getModifiableEnvironment().put("BONG_target.local-temp-folder", "localTemp");

        // successful required keys config
        GCSConfiguration configuration = GCSConfiguration.create();

        assertEquals("topic", configuration.topic());
        assertEquals("bucket", configuration.bucketName());
        assertEquals("gs_secret.json", configuration.gcsServiceAccountKeyFile());
        assertEquals("localTemp", configuration.localTempFolder());
    }
}
