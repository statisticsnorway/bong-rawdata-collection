package no.ssb.dc.collection.client;

import no.ssb.dc.collection.api.config.BootstrapConfiguration;
import no.ssb.dc.collection.api.config.GCSConfiguration;
import no.ssb.dc.collection.api.config.LocalFileSystemConfiguration;
import no.ssb.dc.collection.api.config.SourceLmdbConfiguration;
import no.ssb.dc.collection.api.config.SourceNoDbConfiguration;
import no.ssb.dc.collection.api.config.SourcePostgresConfiguration;
import no.ssb.dc.collection.api.config.TargetConfiguration;
import no.ssb.dc.collection.api.target.RawdataGCSTestWrite;
import no.ssb.dc.collection.api.worker.CsvDynamicWorker;
import no.ssb.dc.collection.api.worker.CsvSpecification;
import no.ssb.dc.collection.api.worker.SpecificationDeserializer;
import no.ssb.dc.collection.bong.rema.RemaBongWorker;
import no.ssb.dc.collection.bong.rema.SourceRemaConfiguration;
import no.ssb.dc.collection.kostra.KostraWorker;
import no.ssb.dc.collection.kostra.SourceKostraConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private static CsvSpecification specification;

    private static Collection<Command> initializeCommands(BootstrapConfiguration configuration, Map<String, String> overrideConfig, Callback printCommands) {
        TargetConfiguration targetConfiguration = configuration.isHelpAction() ?
                null :
                (configuration.useGCSConfiguration() ? GCSConfiguration.create(overrideConfig) : LocalFileSystemConfiguration.create(overrideConfig));

        specification = getSpecification(configuration);

        return List.of(
                new Command("test-gcs-write", null, () -> {
                    LOG.info("Copy dummy rawdata to bucket (ping test).");
                    new RawdataGCSTestWrite().produceRawdataToGCS(Optional.ofNullable(targetConfiguration).orElseThrow(() -> new RuntimeException("TargetConfiguration was not found!")));
                }),
                new Command("produce", "dynamic-no-cache", () -> {
                    try (var worker = new CsvDynamicWorker(SourceNoDbConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.produce();
                    }
                }),
                new Command("generate", "dynamic-no-cache", () -> {
                    try (var worker = new CsvDynamicWorker(SourceNoDbConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.produce();
                    }
                }),
                new Command("prepare", "dynamic-lmdb", () -> {
                    try (var worker = new CsvDynamicWorker(SourceLmdbConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "dynamic-lmdb", () -> {
                    try (var worker = new CsvDynamicWorker(SourceLmdbConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.produce();
                    }
                }),
                new Command("generate", "dynamic-lmdb", () -> {
                    try (var worker = new CsvDynamicWorker(SourceLmdbConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.prepare();
                        worker.produce();
                    }
                }),
                new Command("prepare", "dynamic-postgres", () -> {
                    try (var worker = new CsvDynamicWorker(SourcePostgresConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.prepare();
                    }
                }),
                new Command("produce", "dynamic-postgres", () -> {
                    try (var worker = new CsvDynamicWorker(SourcePostgresConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.produce();
                    }
                }),
                new Command("generate", "dynamic-postgres", () -> {
                    try (var worker = new CsvDynamicWorker(SourcePostgresConfiguration.create(overrideConfig), targetConfiguration, specification)) {
                        worker.prepare();
                        worker.produce();
                    }
                }),
                // TODO deprecate custom workers
                new Command("produce", "kostra", () -> {
                    try (KostraWorker worker = new KostraWorker(SourceKostraConfiguration.create(overrideConfig), Optional.ofNullable(targetConfiguration).orElseThrow(() -> new RuntimeException("TargetConfiguration was not found!")))) {
                        if (!worker.validate()) {
                            return;
                        }
                        worker.produce();
                    }
                }),
                new Command("produce", "rema-fs", () -> {
                    try (RemaBongWorker worker = new RemaBongWorker(SourceRemaConfiguration.create(overrideConfig), Optional.ofNullable(targetConfiguration).orElseThrow(() -> new RuntimeException("TargetConfiguration was not found!")))) {
                        if (!worker.validate()) {
                            return;
                        }
                        worker.produce();
                    }
                }),
                new Command("help", null, printCommands)
        );
    }

    static CsvSpecification getSpecification(BootstrapConfiguration configuration) {
        SpecificationDeserializer deserializer = new SpecificationDeserializer();
        try {
            if (!(configuration.hasSpecificationFilePath() && configuration.hasSpecificationFile())) {
                return null;
            }

            Path specificationFilenamePath = Paths.get(configuration.specificationFilePath()).toAbsolutePath().normalize().resolve(configuration.specificationFile());
            if (!Files.isReadable(specificationFilenamePath)) {
                throw new IllegalStateException("Specification file not found: " + specificationFilenamePath.toString());
            }

            return deserializer.parse(Files.readString(specificationFilenamePath));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final BootstrapConfiguration configuration;
    private final Collection<Command> commands;
    private final AtomicBoolean completed = new AtomicBoolean();

    private Application(BootstrapConfiguration configuration, Map<String, String> overrideConfig) {
        this.configuration = configuration;
        this.commands = initializeCommands(configuration, overrideConfig, this::printCommands);
    }

    boolean isAction(String action) {
        return configuration.hasAction() && configuration.action().equals(action);
    }

    boolean isTarget(String target) {
        return (specification != null && specification.backend != null && target.equals("dynamic-".concat(specification.backend.provider)))
                || (configuration.hasTarget() && configuration.target().equals(target));
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

        LOG.info("Rawdata Client Provider: {}", configuration.rawdataClientProvider());

        String overrideTarget = specification != null && specification.backend != null ?
                "dynamic-" + specification.backend.provider :
                null;

        for (Command command : commands) {
            String target = overrideTarget == null ?
                    command.target :
                    (overrideTarget.equals(command.target) ? overrideTarget : command.target);

            if ((isAction(command.action) && target == null) || (isAction(command.action) && isTarget(target))) {
                valid = true;
                LOG.info("Execute action: {} and target: {} with callback: {}", command.action, target, command.callback);
                command.callback.execute();
                completed.set(true);
                break;
            }
        }

        if (!valid) {
            LOG.warn("No action found!");
        }
    }

    static Application create() {
        return new Application(BootstrapConfiguration.create(), new LinkedHashMap<>());
    }

    static Application create(Map<String, String> overrideConfig) {
        return new Application(BootstrapConfiguration.create(overrideConfig), overrideConfig);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Command command = (Command) o;
            return action.equals(command.action) &&
                    Objects.equals(target, command.target);
        }

        @Override
        public int hashCode() {
            return Objects.hash(action, target);
        }
    }

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        Application application = Application.create();
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
