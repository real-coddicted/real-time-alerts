package io.alerting.config;

import io.alerting.core.EngineConfig;
import io.alerting.core.TimeBasis;
import io.alerting.json.Json;
import io.alerting.json.JsonAccess;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The whole job described as data. Domain specifics live only in {@code sources} and {@code streams}; the
 * rest is shared plumbing. {@code ${VAR}} / {@code ${VAR:-default}} are replaced from the environment (or
 * from {@code overrides}) before parsing.
 *
 * <pre>{@code
 * {
 *   "engine":  { "outputIntervalMs": 1000, "keepTicking": true, "zone": "America/New_York", ... },
 *   "rules":   { "type": "http", "url": "${RULES_URL}", "refreshMs": 30000 },
 *   "kafka":   { "bootstrapServers": "${KAFKA_BOOTSTRAP}", "groupId": "my-alerting",
 *                "consumer": {...extra props}, "producer": {...extra props} },
 *   "sources": [ { "name": "tick-pnl", "topic": "luna.tick_pnl.events", "format": "json",
 *                  "offsetReset": "latest" } ],
 *   "streams": [ ...see CompiledStream... ],
 *   "sink":    { "topic": "luna.risk_alert.events" }
 * }
 * }</pre>
 *
 * Serializable as raw JSON so it can travel inside DoFns; components are compiled on workers.
 */
public final class PipelineConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern VAR = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_.-]*)(?::-([^}]*))?}");

    private final String location;
    private final String json;
    private transient Map<String, Object> root;

    private PipelineConfig(String location, String json) {
        this.location = location;
        this.json = json;
        root();
        validate();
    }

    public static PipelineConfig load(String location, Map<String, String> overrides) throws IOException {
        return new PipelineConfig(location, interpolate(Resources.read(location), overrides));
    }

    public static PipelineConfig fromJson(String json) {
        return new PipelineConfig(null, interpolate(json, Map.of()));
    }

    private Map<String, Object> root() {
        if (root == null) root = Json.parseObject(json);
        return root;
    }

    private void validate() {
        Set<String> sources = new HashSet<>();
        for (Map<String, Object> s : sources()) {
            if (!sources.add(JsonAccess.requireString(s, "name"))) {
                throw new IllegalArgumentException("duplicate source name: " + s.get("name"));
            }
        }
        Set<String> streams = new HashSet<>();
        for (Map<String, Object> s : streams()) {
            CompiledStream c = CompiledStream.compile(s);
            if (!streams.add(c.name())) throw new IllegalArgumentException("duplicate stream name: " + c.name());
            if (!sources.contains(c.source())) {
                throw new IllegalArgumentException("stream '" + c.name() + "' reads unknown source '" + c.source() + "'");
            }
        }
        if (streams.isEmpty()) throw new IllegalArgumentException("config needs at least one stream");
        engine();
        JsonAccess.requireMap(root(), "rules");
    }

    public String location() {
        return location;
    }

    public String json() {
        return json;
    }

    public EngineConfig engine() {
        Map<String, Object> m = JsonAccess.getMap(root(), "engine");
        Map<String, Object> rules = JsonAccess.requireMap(root(), "rules");
        EngineConfig.Builder b = EngineConfig.builder()
                .ruleRefreshInterval(Duration.ofMillis(JsonAccess.getLong(rules, "refreshMs", 30_000)));
        if (m == null) return b.build();
        if (m.containsKey("outputIntervalMs")) b.outputInterval(Duration.ofMillis(JsonAccess.getLong(m, "outputIntervalMs", 1000)));
        if (m.containsKey("keepTicking")) b.keepTicking(JsonAccess.getBoolean(m, "keepTicking", false));
        if (m.containsKey("zone")) b.zone(ZoneId.of(JsonAccess.requireString(m, "zone")));
        if (m.containsKey("alertIdBytes")) b.alertIdBytes(JsonAccess.getInt(m, "alertIdBytes", 6));
        if (m.containsKey("timeBasis")) b.timeBasis(TimeBasis.valueOf(JsonAccess.requireString(m, "timeBasis").toUpperCase(Locale.ROOT)));
        if (m.containsKey("dedupeAgainstLastEmitted")) b.dedupeAgainstLastEmitted(JsonAccess.getBoolean(m, "dedupeAgainstLastEmitted", false));
        if (m.containsKey("resolveOnRuleRemoval")) b.resolveOnRuleRemoval(JsonAccess.getBoolean(m, "resolveOnRuleRemoval", true));
        return b.build();
    }

    /** The {@code rules} section; relative file paths are resolved against the config location. */
    public Map<String, Object> rules() {
        Map<String, Object> m = new LinkedHashMap<>(JsonAccess.requireMap(root(), "rules"));
        if ("file".equals(m.get("type")) && m.get("path") != null) {
            m.put("path", Resources.resolve(location, m.get("path").toString()));
        }
        return m;
    }

    public Map<String, Object> kafka() {
        Map<String, Object> m = JsonAccess.getMap(root(), "kafka");
        return m == null ? Map.of() : m;
    }

    public List<Map<String, Object>> sources() {
        return maps("sources", s -> {
            Map<String, Object> m = new LinkedHashMap<>(s);
            if (m.get("descriptorSet") != null) m.put("descriptorSet", Resources.resolve(location, m.get("descriptorSet").toString()));
            return m;
        });
    }

    public List<Map<String, Object>> streams() {
        return maps("streams", Function.identity());
    }

    public Map<String, Object> sink() {
        return JsonAccess.requireMap(root(), "sink");
    }

    private List<Map<String, Object>> maps(String key, Function<Map<String, Object>, Map<String, Object>> f) {
        List<Object> list = JsonAccess.getList(root(), key);
        if (list == null) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : list) out.add(f.apply(JsonAccess.asMap(o, key + " entry")));
        return Collections.unmodifiableList(out);
    }

    static String interpolate(String text, Map<String, String> overrides) {
        Matcher mt = VAR.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (mt.find()) {
            String name = mt.group(1);
            String v = overrides.containsKey(name) ? overrides.get(name) : System.getenv(name);
            if (v == null) v = mt.group(2);
            if (v == null) throw new IllegalArgumentException("config references ${" + name + "} but it is not set");
            mt.appendReplacement(sb, Matcher.quoteReplacement(v));
        }
        mt.appendTail(sb);
        return sb.toString();
    }
}
