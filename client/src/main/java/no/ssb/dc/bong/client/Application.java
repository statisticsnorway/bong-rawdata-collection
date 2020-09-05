package no.ssb.dc.bong.client;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.bong.commons.config.GCSConfiguration;
import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.ng.ping.RawdataGCSTestWrite;
import no.ssb.dc.bong.ng.repository.NGLmdbBongRepository;
import no.ssb.dc.bong.ng.repository.NGPostgresBongRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    final DynamicConfiguration configuration;
    final AtomicBoolean success = new AtomicBoolean();

    private Application(DynamicConfiguration configuration) {
        this.configuration = configuration;

        if (isAction("ping.test")) {
            doPingTest();

        } else if (isAction("buildDatabase") && isTarget("ng.lmdb")) {
            buildNGLmdbDatabase();

        } else if (isAction("produceRawdata") && isTarget("ng.lmdb")) {
            produceNGLmdbRatadata();

        } else if (isAction("buildDatabase") && isTarget("ng.postgres")) {
            buildNGPostgresDatabase();

        } else if (isAction("produceRawdata") && isTarget("ng.postgres")) {
            produceNGPostgresRatadata();
        }

        success.set(true);
    }

    boolean isAction(String action) {
        return configuration.evaluateToString("action") != null && configuration.evaluateToString("action").equals(action);
    }

    boolean isTarget(String target) {
        return configuration.evaluateToString("target") != null && configuration.evaluateToString("target").equals(target);
    }

    void doPingTest() {
        LOG.info("Copy dummy rawdata to bucket (ping test).");
        GCSConfiguration gcsConfiguration = new GCSConfiguration();
        RawdataGCSTestWrite rawdataGCSTestWrite = new RawdataGCSTestWrite();
        rawdataGCSTestWrite.produceRawdataToGCS(gcsConfiguration);
    }

    void buildNGLmdbDatabase() {
        NGLmdbBongRepository repository = new NGLmdbBongRepository(new SourceLmdbConfiguration(), new GCSConfiguration());
        repository.buildDatabase();
    }

    void produceNGLmdbRatadata() {
        NGLmdbBongRepository repository = new NGLmdbBongRepository(new SourceLmdbConfiguration(), new GCSConfiguration());
        repository.produceRawdata();
    }

    void buildNGPostgresDatabase() {
        NGPostgresBongRepository repository = new NGPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration());
        repository.buildDatabase();
    }

    void produceNGPostgresRatadata() {
        NGPostgresBongRepository repository = new NGPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration());
        repository.produceRawdata();
    }

    public static Application run(DynamicConfiguration configuration) {
        return new Application(configuration);
    }

    /*
        usage:
            docker run -it --rm -e BONG_ping.test= bong-collection:dev
     */

    public static class CLI implements Runnable {
        final DynamicConfiguration configuration;
        final AtomicBoolean success = new AtomicBoolean();

        public CLI(DynamicConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public void run() {
            Application app = Application.run(configuration);
            success.set(app.success.get());
        }
    }

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        StoreBasedDynamicConfiguration.Builder configurationBuilder = new StoreBasedDynamicConfiguration.Builder()
                .environment("BONG_")
                .systemProperties();

        CLI cli = new CLI(configurationBuilder.build());
        Thread application = new Thread(cli);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!cli.success.get()) {
                    LOG.warn("ShutdownHook triggered..");
                }
                application.interrupt();
            }));

            application.start();

            long time = System.currentTimeMillis() - now;
            LOG.info("Client started in {}ms..", time);

            // wait for termination signal
            try {
                application.join();
            } catch (InterruptedException e) {
            }
        } finally {
            LOG.info("Done!");
        }
    }

}
