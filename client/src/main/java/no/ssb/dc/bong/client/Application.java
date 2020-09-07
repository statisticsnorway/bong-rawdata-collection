package no.ssb.dc.bong.client;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.bong.commons.config.GCSConfiguration;
import no.ssb.dc.bong.commons.config.SourceLmdbConfiguration;
import no.ssb.dc.bong.commons.config.SourcePostgresConfiguration;
import no.ssb.dc.bong.coop.CoopPostgresBongRepository;
import no.ssb.dc.bong.ng.ping.RawdataGCSTestWrite;
import no.ssb.dc.bong.ng.repository.NGLmdbBongRepository;
import no.ssb.dc.bong.ng.repository.NGPostgresBongRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DynamicConfiguration captures environment variables set to docker and executes a Command.
 * <p>
 * usage:
 * docker run -it --rm -e BONG_ping.test= bong-collection:dev
 */
public class Application implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private static final Collection<Command> commands = List.of(
            new Command("test-gcs-write", null, () -> {
                LOG.info("Copy dummy rawdata to bucket (ping test).");
                new RawdataGCSTestWrite().produceRawdataToGCS(new GCSConfiguration());
            }),
            new Command("build-database", "ng-lmdb", () -> new NGLmdbBongRepository(new SourceLmdbConfiguration(), new GCSConfiguration()).prepare()),
            new Command("produce-rawdata", "ng-lmdb", () -> new NGLmdbBongRepository(new SourceLmdbConfiguration(), new GCSConfiguration()).produce()),
            new Command("build-database", "ng-postgres", () -> new NGPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration()).prepare()),
            new Command("produce-rawdata", "ng-postgres", () -> new NGPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration()).produce()),
            new Command("build-database", "coop-postgres", () -> new CoopPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration()).prepare()),
            new Command("produce-rawdata", "coop-postgres", () -> new CoopPostgresBongRepository(new SourcePostgresConfiguration(), new GCSConfiguration()).produce())
    );

    private final DynamicConfiguration configuration;
    private final AtomicBoolean completed = new AtomicBoolean();

    private Application(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    boolean isAction(String action) {
        return configuration.evaluateToString("action") != null && configuration.evaluateToString("action").equals(action);
    }

    boolean isTarget(String target) {
        return configuration.evaluateToString("target") != null && configuration.evaluateToString("target").equals(target);
    }

    @Override
    public void run() {
        boolean valid = false;

        for (Command command : commands) {
            if ((isAction(command.action) && command.target == null) || (isAction(command.action) && isTarget(command.target))) {
                valid = true;
                LOG.info("Execute action: {} and target: {}", command.action, command.target);
                command.callback.execute();
                completed.set(true);
                break;
            }
        }

        if (!valid) {
            LOG.warn("No action found!");
        }
    }

    static Application create(DynamicConfiguration configuration) {
        return new Application(configuration);
    }

    @FunctionalInterface
    interface Callback {
        void execute();
    }

    static class Command {
        final String action;
        final String target;
        final Callback callback;

        Command(String action, String target, Callback callback) {
            this.action = action;
            this.target = target;
            this.callback = callback;
        }
    }

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .environment("BONG_")
                .systemProperties()
                .build();


        Application application = Application.create(configuration);
        Thread thread = new Thread(application);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!application.completed.get()) {
                    LOG.warn("ShutdownHook triggered..");
                }
                thread.interrupt();
            }));

            thread.start();

            long time = System.currentTimeMillis() - now;
            LOG.info("Client started in {}ms..", time);

            // wait for termination signal
            try {
                thread.join();
            } catch (InterruptedException e) {
                // suppress
            }
        } finally {
            long time = System.currentTimeMillis() - now;
            LOG.info("Completed in {}ms!", time);
        }
    }
}
