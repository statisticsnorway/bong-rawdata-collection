package no.ssb.dc.collection.client;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.collection.api.config.GCSConfiguration;
import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.bong.coop.CoopPostgresBongWorker;
import no.ssb.dc.collection.bong.ng.NGLmdbBongWorker;
import no.ssb.dc.collection.bong.ng.NGPostgresBongWorker;
import no.ssb.dc.collection.bong.ng.RawdataGCSTestWrite;
import no.ssb.dc.collection.bong.rema.RemaBongWorker;
import no.ssb.dc.collection.bong.rema.SourceRemaConfiguration;
import no.ssb.dc.collection.kag.BefolkningWorker;
import no.ssb.dc.collection.kag.FagkodeWorker;
import no.ssb.dc.collection.kag.GsiWorker;
import no.ssb.dc.collection.kag.KarakterWorker;
import no.ssb.dc.collection.kag.NasjonaleProverWorker;
import no.ssb.dc.collection.kag.NudbWorker;
import no.ssb.dc.collection.kag.NuskatWorker;
import no.ssb.dc.collection.kag.OmkodingskatalogWorker;
import no.ssb.dc.collection.kag.ResultatWorker;
import no.ssb.dc.collection.kag.SkolekatalogWorker;
import no.ssb.dc.collection.kag.StatistikkWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DynamicConfiguration captures environment variables set to docker and executes a Command.
 * <p>
 * usage:
 * docker run -it --rm -e BONG_test-gcs-write= bong-collection:dev
 */
public class Application implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    static final String DEBUG_CONFIG_OVERRIDE = "rawdata-client-debug-config-override";

    private static Collection<Command> initializeCommands(Map<String, String> configMap, Callback printCommands) {
        boolean useGCSProvider = configMap.containsKey("target.rawdata.client.provider") && configMap.get("target.rawdata.client.provider").equals("gcs");
        boolean printHelp = configMap.containsKey("action") && configMap.get("action").equals("help") && (!configMap.containsKey("target") || configMap.get("target") == null || "".equals(configMap.get("target")));
        TargetConfiguration targetConfiguration = !printHelp ?
                (useGCSProvider ? new GCSConfiguration(configMap) : new LocalFileSystemConfiguration(configMap)) :
                null;
        return List.of(
                new Command("test-gcs-write", null, () -> {
                    LOG.info("Copy dummy rawdata to bucket (ping test).");
                    new RawdataGCSTestWrite().produceRawdataToGCS(targetConfiguration);
                }),
                new Command("prepare", "ng-lmdb", () -> {
                    try (var worker = new NGLmdbBongWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "ng-lmdb", () -> {
                    try (var worker = new NGLmdbBongWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "ng-postgres", () -> {
                    try (var worker = new NGPostgresBongWorker(new SourcePostgresConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "ng-postgres", () -> {
                    try (var worker = new NGPostgresBongWorker(new SourcePostgresConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "coop-postgres", () -> {
                    try (var worker = new CoopPostgresBongWorker(new SourcePostgresConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "coop-postgres", () -> {
                    try (var worker = new CoopPostgresBongWorker(new SourcePostgresConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("produce", "rema-fs", () -> {
                    try (RemaBongWorker worker = new RemaBongWorker(new SourceRemaConfiguration(), targetConfiguration)) {
                        if (!worker.validate()) {
                            return;
                        }
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-karakter-lmdb", () -> {
                    try (var worker = new KarakterWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-karakter-lmdb", () -> {
                    try (var worker = new KarakterWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-resultat-lmdb", () -> {
                    try (var worker = new ResultatWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-resultat-lmdb", () -> {
                    try (var worker = new ResultatWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-fagkode-lmdb", () -> {
                    try (var worker = new FagkodeWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-fagkode-lmdb", () -> {
                    try (var worker = new FagkodeWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-statistikk-lmdb", () -> {
                    try (var worker = new StatistikkWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-statistikk-lmdb", () -> {
                    try (var worker = new StatistikkWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-nuskat-lmdb", () -> {
                    try (var worker = new NuskatWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-nuskat-lmdb", () -> {
                    try (var worker = new NuskatWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-skolekatalog-lmdb", () -> {
                    try (var worker = new SkolekatalogWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-skolekatalog-lmdb", () -> {
                    try (var worker = new SkolekatalogWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-omkodingskatalog-lmdb", () -> {
                    try (var worker = new OmkodingskatalogWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-omkodingskatalog-lmdb", () -> {
                    try (var worker = new OmkodingskatalogWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-gsi-lmdb", () -> {
                    try (var worker = new GsiWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-gsi-lmdb", () -> {
                    try (var worker = new GsiWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-nudb-lmdb", () -> {
                    try (var worker = new NudbWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-nudb-lmdb", () -> {
                    try (var worker = new NudbWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-befolkning-lmdb", () -> {
                    try (var worker = new BefolkningWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-befolkning-lmdb", () -> {
                    try (var worker = new BefolkningWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "kag-nasjonale-prover-lmdb", () -> {
                    try (var worker = new NasjonaleProverWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "kag-nasjonale-prover-lmdb", () -> {
                    try (var worker = new NasjonaleProverWorker(new SourceLmdbConfiguration(), targetConfiguration)) {
                        worker.produce();
                    }
                }),
                new Command("help", null, printCommands)
        );
    }

    private final DynamicConfiguration configuration;
    private final Collection<Command> commands;
    private final AtomicBoolean completed = new AtomicBoolean();

    private Application(DynamicConfiguration configuration) {
        this.configuration = configuration;
        this.commands = initializeCommands(configuration.asMap(), this::printCommands);
    }

    boolean isAction(String action) {
        return configuration.evaluateToString("action") != null && configuration.evaluateToString("action").equals(action);
    }

    boolean isTarget(String target) {
        return configuration.evaluateToString("target") != null && configuration.evaluateToString("target").equals(target);
    }

    void printCommands() {
        StringBuilder builder = new StringBuilder();
        builder.append("\t").append(String.format("%-20s%-40s%n", "Action", "Target"));
        builder.append("\t").append(String.format("%-20s%-40s%n", "------", "------"));
        commands.forEach(command -> builder.append("\t").append(String.format("%-20s%-40s%n", command.action, command.target == null ? "(none)" : command.target)));
        System.out.printf("Commands:%n%s", builder.toString());
    }

    @Override
    public void run() {
        boolean valid = false;

        LOG.info("Rawdata Client Provider: {}", configuration.evaluateToString("target.rawdata.client.provider"));

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
