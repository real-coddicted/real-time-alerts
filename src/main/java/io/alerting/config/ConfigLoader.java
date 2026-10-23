package io.alerting.config;

import io.alerting.engine.EngineConfig;
import io.alerting.engine.TimeBasis;
import io.alerting.json.Json;
import io.alerting.json.JsonAccess;
import io.alerting.rules.HttpRuleProvider;
import io.alerting.rules.JsonFileRuleProvider;
import io.alerting.rules.RuleProvider;
import io.alerting.sink.AlertSink;
import io.alerting.sink.JsonLinesSink;
import io.alerting.source.JsonLinesSource;
import io.alerting.source.Source;
import io.alerting.stream.StreamDefinition;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Loads an {@link EngineSetup} from a JSON config file. Relative paths resolve against the file's directory. */
public final class ConfigLoader {
    private static final Pattern ENV = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?}");

    private ConfigLoader() {}

    public static EngineSetup load(Path file) throws IOException {
        Map<String, Object> root = Json.parseObject(Files.readString(file));
        Path base = file.toAbsolutePath().getParent();

        Map<String, Object> rulesSpec = JsonAccess.requireMap(root, "rules");
        EngineConfig config = engineConfig(JsonAccess.getMap(root, "engine"), rulesSpec);
        RuleProvider rules = ruleProvider(rulesSpec, base);

        List<Source> sources = new ArrayList<>();
        List<Object> sourceSpecs = JsonAccess.getList(root, "sources");
        if (sourceSpecs != null) {
            for (Object s : sourceSpecs) sources.add(source(JsonAccess.asMap(s, "source"), base));
        }

        List<StreamDefinition<?>> streams = new ArrayList<>();
        List<Object> streamSpecs = JsonAccess.getList(root, "streams");
        if (streamSpecs == null || streamSpecs.isEmpty()) throw new IllegalArgumentException("config needs at least one stream");
        Set<String> names = new HashSet<>();
        for (Object s : streamSpecs) {
            StreamDefinition<?> def = ConfigStreams.compile(JsonAccess.asMap(s, "stream"));
            if (!names.add(def.name())) throw new IllegalArgumentException("duplicate stream name: " + def.name());
            streams.add(def);
        }

        Map<String, Object> sinkSpec = JsonAccess.getMap(root, "sink");
        AlertSink sink = sink(sinkSpec == null ? Map.of("type", "jsonl", "path", "-") : sinkSpec, base);
        return new EngineSetup(config, rules, sources, streams, sink);
    }

    static EngineConfig engineConfig(Map<String, Object> m, Map<String, Object> rulesSpec) {
        EngineConfig.Builder b = EngineConfig.builder();
        b.ruleRefreshInterval(Duration.ofMillis(JsonAccess.getLong(rulesSpec, "refreshMs", 30_000)));
        if (m == null) return b.build();
        if (m.containsKey("outputIntervalMs")) b.outputInterval(Duration.ofMillis(JsonAccess.getLong(m, "outputIntervalMs", 1000)));
        if (m.containsKey("keepTicking")) b.keepTicking(JsonAccess.getBoolean(m, "keepTicking", false));
        if (m.containsKey("zone")) b.zone(ZoneId.of(JsonAccess.requireString(m, "zone")));
        if (m.containsKey("alertIdBytes")) b.alertIdBytes(JsonAccess.getInt(m, "alertIdBytes", 6));
        if (m.containsKey("shards")) b.shards(JsonAccess.getInt(m, "shards", 1));
        if (m.containsKey("timeBasis")) b.timeBasis(TimeBasis.valueOf(JsonAccess.requireString(m, "timeBasis").toUpperCase(Locale.ROOT)));
        if (m.containsKey("dedupeAgainstLastEmitted")) b.dedupeAgainstLastEmitted(JsonAccess.getBoolean(m, "dedupeAgainstLastEmitted", false));
        if (m.containsKey("resolveOnRuleRemoval")) b.resolveOnRuleRemoval(JsonAccess.getBoolean(m, "resolveOnRuleRemoval", true));
        if (m.containsKey("maxPendingPerShard")) b.maxPendingPerShard(JsonAccess.getInt(m, "maxPendingPerShard", 100_000));
        return b.build();
    }

    static RuleProvider ruleProvider(Map<String, Object> m, Path base) {
        String type = JsonAccess.requireString(m, "type");
        return switch (type) {
            case "file" -> new JsonFileRuleProvider(base.resolve(JsonAccess.requireString(m, "path")));
            case "http" -> {
                Map<String, String> headers = new LinkedHashMap<>();
                Map<String, Object> h = JsonAccess.getMap(m, "headers");
                if (h != null) h.forEach((k, v) -> headers.put(k, interpolate(String.valueOf(v))));
                yield new HttpRuleProvider(
                        URI.create(interpolate(JsonAccess.requireString(m, "url"))),
                        headers,
                        JsonAccess.getInt(m, "pageSize", 1000),
                        Duration.ofMillis(JsonAccess.getLong(m, "connectTimeoutMs", 60_000)),
                        Duration.ofMillis(JsonAccess.getLong(m, "readTimeoutMs", 120_000)),
                        JsonAccess.getInt(m, "maxAttempts", 5),
                        Duration.ofMillis(JsonAccess.getLong(m, "initialBackoffMs", 2_000)),
                        Duration.ofMillis(JsonAccess.getLong(m, "maxBackoffMs", 30_000)));
            }
            default -> throw new IllegalArgumentException("unknown rules type: " + type);
        };
    }

    static Source source(Map<String, Object> m, Path base) {
        String type = JsonAccess.requireString(m, "type");
        String name = JsonAccess.requireString(m, "name");
        return switch (type) {
            case "jsonl", "jsonl-mux" -> {
                String path = JsonAccess.getString(m, "path", "-");
                yield new JsonLinesSource(name, "-".equals(path) ? null : base.resolve(path),
                        JsonAccess.getBoolean(m, "follow", false),
                        type.equals("jsonl-mux"),
                        JsonAccess.getLong(m, "lineDelayMs", 0));
            }
            default -> throw new IllegalArgumentException("unknown source type: " + type + " (implement io.alerting.source.Source for others)");
        };
    }

    static AlertSink sink(Map<String, Object> m, Path base) throws IOException {
        String type = JsonAccess.requireString(m, "type");
        if (!type.equals("jsonl")) throw new IllegalArgumentException("unknown sink type: " + type + " (implement io.alerting.sink.AlertSink for others)");
        String path = JsonAccess.getString(m, "path", "-");
        if ("-".equals(path)) return new JsonLinesSink(System.out, false);
        return new JsonLinesSink(Files.newOutputStream(base.resolve(path), StandardOpenOption.CREATE, StandardOpenOption.APPEND), true);
    }

    /** Replaces {@code ${VAR}} / {@code ${VAR:-default}} with environment values. */
    static String interpolate(String s) {
        Matcher mt = ENV.matcher(s);
        StringBuilder sb = new StringBuilder();
        while (mt.find()) {
            String v = System.getenv(mt.group(1));
            if (v == null) v = mt.group(2) != null ? mt.group(2) : "";
            mt.appendReplacement(sb, Matcher.quoteReplacement(v));
        }
        mt.appendTail(sb);
        return sb.toString();
    }
}
