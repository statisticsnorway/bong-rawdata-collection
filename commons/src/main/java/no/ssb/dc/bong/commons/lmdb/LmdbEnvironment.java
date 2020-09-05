package no.ssb.dc.bong.commons.lmdb;

import no.ssb.config.DynamicConfiguration;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbEnvironment implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LmdbEnvironment.class);

    private final Path databaseDir;
    private final Env<ByteBuffer> env;
    private final String topic;
    private boolean dropDatabase;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long mapSize;
    private final int numberOfDbs;
    private Dbi<ByteBuffer> db;

    public LmdbEnvironment(DynamicConfiguration configuration, boolean dropDatabase) {
        this.topic = configuration.evaluateToString("rawdata.topic");
        this.dropDatabase = dropDatabase;
        String databasePath = configuration.evaluateToString("lmdb.path");
        if (databasePath.contains("$PROJECT_DIR")) {
            databasePath = databasePath.replace("$PROJECT_DIR", Paths.get(".").normalize().resolve(databasePath).toString());
        }
        this.databaseDir = Paths.get(databasePath).resolve(topic);
        LOG.debug("DbPath: {}", databaseDir);
        if (dropDatabase && databaseDir.toFile().exists()) {
            removePath(databaseDir);
        }
        createDirectories(this.databaseDir);
        mapSize = configuration != null && configuration.evaluateToString("lmdb.sizeInMb") != null ?
                configuration.evaluateToInt("lmdb.sizeInMb") : 50;
        numberOfDbs = configuration != null && configuration.evaluateToString("lmdb.numberOfDbs") != null ?
                configuration.evaluateToInt("lmdb.numberOfDbs") : 1;
        env = createEnvironment();
    }

    public static void removePath(Path path) {
        try {
            if (path.toFile().exists())
                Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Env<ByteBuffer> env() {
        return env;
    }

    public int maxKeySize() {
        return env.getMaxKeySize();
    }

    public Path getDatabaseDir() {
        return databaseDir;
    }

    private void createDirectories(Path databaseDir) {
        if (!databaseDir.toFile().exists()) {
            try {
                Files.createDirectories(databaseDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Env<ByteBuffer> createEnvironment() {
        long dbSize = mapSize * 1024 * 1024;
        LOG.info("{}} Lmdb database with numberOfDbs: {}, mapSize: {} MiB, dbSize: {} bytes", (dropDatabase ? "Create" : "Open"), numberOfDbs, mapSize, dbSize);
        return Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(dbSize)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(numberOfDbs)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(databaseDir.toFile(), EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC);
    }

    public Dbi<ByteBuffer> open() {
        if (!closed.get() && db != null) {
            return db;
        }
        db = env.openDbi(topic, MDB_CREATE);
        return db;
    }

    void drop() {
        if (!closed.get() && db != null) {
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                db.drop(txn);
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        // drop()
        if (closed.compareAndSet(false, true)) {
            env.close();
        }
    }
}
