package io.alerting;

import io.alerting.config.ConfigLoader;
import io.alerting.config.EngineSetup;
import io.alerting.engine.AlertEngine;
import io.alerting.json.Json;

import java.nio.file.Path;
import java.time.Duration;

/**
 * {@code java -jar alerting-engine.jar --config engine.json [--await-rules-ms 60000]}
 *
 * <p>Runs until all sources finish (then drains and exits) or until killed. Alerts go to the configured
 * sink; logs and final metrics go to stderr.
 */
public final class Main {
    static {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tFT%1$tT.%1$tL %4$-7s %3$s - %5$s%6$s%n");
        }
    }

    private Main() {}

    public static void main(String[] args) throws Exception {
        Path config = null;
        long awaitRulesMs = 60_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> config = Path.of(args[++i]);
                case "--await-rules-ms" -> awaitRulesMs = Long.parseLong(args[++i]);
                case "-h", "--help" -> {
                    usage();
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    usage();
                    System.exit(2);
                }
            }
        }
        if (config == null) {
            usage();
            System.exit(2);
        }

        EngineSetup setup = ConfigLoader.load(config);
        AlertEngine engine = setup.toEngine();
        Runtime.getRuntime().addShutdownHook(new Thread(engine::close, "shutdown"));
        engine.start(Duration.ofMillis(awaitRulesMs));
        engine.awaitSources(null);
        engine.drain(Duration.ofSeconds(30));
        System.err.println("metrics " + Json.write(engine.metrics().snapshot()));
        engine.close();
    }

    private static void usage() {
        System.err.println("usage: alerting-engine --config <engine.json> [--await-rules-ms <ms>]");
    }
}
