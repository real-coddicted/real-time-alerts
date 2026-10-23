package io.alerting.source;

import io.alerting.json.Json;
import io.alerting.json.JsonAccess;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Reads one JSON document per line from a file or stdin. Blank lines and lines starting with {@code //} are
 * skipped; unparseable lines are logged and skipped.
 *
 * <p>In multiplexed mode each line is an envelope {@code {"source": "...", "record": {...}, "timestamp": 123}}
 * and the record is routed to the named source, so one file can replay several topics in order.
 * With {@code follow} the file is tailed like {@code tail -f}.
 */
public final class JsonLinesSource implements Source {
    private static final System.Logger LOG = System.getLogger(JsonLinesSource.class.getName());

    private final String name;
    private final Path path;
    private final boolean follow;
    private final boolean multiplexed;
    private final long lineDelayMs;

    /** @param path null reads stdin */
    public JsonLinesSource(String name, Path path, boolean follow, boolean multiplexed, long lineDelayMs) {
        this.name = name;
        this.path = path;
        this.follow = follow;
        this.multiplexed = multiplexed;
        this.lineDelayMs = lineDelayMs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        try (BufferedReader in = path == null
                ? new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
                : Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            long lineNo = 0;
            while (ctx.isRunning()) {
                String line = in.readLine();
                if (line == null) {
                    if (!follow) return;
                    Thread.sleep(200);
                    continue;
                }
                lineNo++;
                String trimmed = line.strip();
                if (trimmed.isEmpty() || trimmed.startsWith("//")) continue;
                try {
                    emit(ctx, Json.parse(trimmed));
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "source " + name + " line " + lineNo + " skipped: " + e.getMessage());
                }
                if (lineDelayMs > 0) Thread.sleep(lineDelayMs);
            }
        }
    }

    private void emit(SourceContext ctx, Object doc) {
        if (!multiplexed) {
            ctx.emit(doc, null);
            return;
        }
        Map<String, Object> env = JsonAccess.asMap(doc, "envelope");
        String target = JsonAccess.requireString(env, "source");
        Object ts = env.get("timestamp");
        ctx.emitTo(target, env.get("record"), ts == null ? null : JsonAccess.getLong(env, "timestamp", 0));
    }
}
