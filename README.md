# Generic Alert Engine

A streaming rule evaluation engine generalized from the `common-alerting` spec. It reads any number of
sources, turns each record into keyed metrics through any number of **streams**, matches them against
user defined **rules**, runs a per-key alert state machine and emits `Alert` transitions
(`FIRED`, `FIRED_UPDATED`, `RESOLVED_AUTOMATICALLY`, `RESOLVED_EXPIRED`, `RESOLVED_RULE_REMOVED`).

The original pipeline hardcoded seven branches, seven evaluator classes and five topic readers. Here they are
data: [`examples/risk/engine.json`](examples/risk/engine.json) reproduces the entire original topology in
config, and the spec's end-to-end scenario (E1..E17) passes against it.

Java 21, zero dependencies (plain `javac`).

```bash
./build.sh
```

```bash
./test.sh
```

```bash
java -jar build/alerting-engine.jar --config examples/risk/engine.json
```

## Architecture

```
 Source ─┐                 ┌─ Stream A: steps → key → subjects ─┐
 Source ─┼─ submit(src,r) ─┼─ Stream B: steps → key → subjects ─┼─▶ shard[hash(stream,key)] ─▶ AlertSink
 Source ─┘                 └─ Stream C ...                      ┘        │ per-key state machine
                                                                          │ throttle · match · evaluate · EOD
                          RuleProvider ─(poll, keep last good)─▶ RuleCatalog (immutable, versioned)
```

| Concept | Class | Spec equivalent |
|---|---|---|
| Source | `source.Source` (`JsonLinesSource`, `IterableSource`, or `engine.submit(...)` from anywhere) | Kafka readers |
| Stream | `stream.StreamDefinition` (Java API) / `config.ConfigStreams` (JSON) | one branch + its `*RuleEvaluator` + `RuleObjectKey.from*` |
| Key | `model.EntityKey{scope, date, attributes}` | `RuleObjectKey` |
| Rule | `model.Rule` | `riskalerts.v1beta1.Rule` |
| Rule algorithm | `rules.RuleEvaluator` (section 6) + `threshold.*` | `RuleEvaluator` template + threshold evaluators |
| State machine | `engine.Shard` (section 5) | `AlertingDoFn` |
| Rules side input | `rules.RuleCatalogHolder` + `RuleProvider` (`JsonFileRuleProvider`, `HttpRuleProvider`, `StaticRuleProvider`) | `RulesProviderDoFn` + `View.asSingleton` |
| Sink | `sink.AlertSink` (`JsonLinesSink`, `InMemorySink`) | `KafkaIO.writeRecords` |

**Concurrency.** State is partitioned into `shards`, each owned by one single-threaded `Scheduler`. Every
event and timer for a `(stream, key)` runs on its shard's thread, so no locks are needed and per-key order
is preserved. `submit` blocks when a shard has `maxPendingPerShard` queued events (back-pressure).
`ManualScheduler` runs the same code on virtual time, which the tests use and which also suits deterministic replays.

## Adding a source or alert type

**In config, no code.** Add a source, then one stream per alert family that reads it:

```json
{
  "name": "cpu-host", "source": "metrics",
  "timestamp": "ts",
  "steps": [ { "filter": { "field": "kind", "op": "eq", "value": "cpu" } } ],
  "key": { "scope": "HOST", "attributes": { "host": "host", "dc": "labels.dc" } },
  "subjects": {
    "CPU_UTIL": { "subject": { "name": "used", "value": "cpu.used" },
                  "compare": { "name": "capacity", "value": "cpu.capacity" } }
  }
}
```

and a rule: `{"id":"cpu-hot","subject":"CPU_UTIL","scope":"HOST","match":{"dc":"use1"},"fire":{"type":"RATIO","operator":"GT","value":"0.9","expiryMs":-1},"resolve":{"type":"RATIO","operator":"LT","value":"0.8"}}`.

**In Java.** Build a typed stream and push records from any transport:

```java
StreamDefinition<PnlResult> pnl = StreamDefinition.builder("pnl-position", "tick-pnl")
    .as(PnlResult.class)
    .key(p -> EntityKey.of("POSITION", p.getDate(), Map.of("unit_id", p.getUnitId(), "instrument_id", p.getInstrumentId().getValue())))
    .timestamp(PnlResult::getTimestamp)
    .subject("TOTAL_PNL", p -> Metric.of("total_pnl", p.getTotalPnl(), "sod_equity", p.getSodEquity()))
    .build();

AlertEngine engine = AlertEngine.builder()
    .config(EngineConfig.builder().keepTicking(true).build())
    .ruleProvider(new HttpRuleProvider(...))
    .stream(pnl)
    .sink(myKafkaSink)          // implement AlertSink
    .build();
engine.start(Duration.ofSeconds(60));
consumer.poll(...).forEach(r -> engine.submit("tick-pnl", r.value(), r.timestamp()));
```

Custom threshold types: implement `ThresholdEvaluator` and pass `ChainedThresholdEvaluator.standard(myType)` to `.thresholds(...)`.

## Config reference

```jsonc
{
  "engine": {
    "outputIntervalMs": 1000,        // per-key throttle; latest event in the window is evaluated
    "keepTicking": false,            // FIRED_UPDATED for active alerts even below the fire threshold
    "zone": "America/New_York",      // business day for end-of-day expiry
    "alertIdBytes": 6,
    "shards": 8,
    "timeBasis": "EVENT",            // EVENT (spec) or PROCESSING
    "dedupeAgainstLastEmitted": false,
    "resolveOnRuleRemoval": true,
    "maxPendingPerShard": 100000
  },
  "rules": { "type": "file", "path": "rules.json", "refreshMs": 30000 },
  // or { "type": "http", "url": "https://.../v1beta1/rules", "headers": {"Authorization": "Bearer ${TOKEN}"},
  //      "pageSize": 1000, "connectTimeoutMs": 60000, "readTimeoutMs": 120000,
  //      "maxAttempts": 5, "initialBackoffMs": 2000, "maxBackoffMs": 30000 }
  "sources": [
    { "name": "tick-pnl", "type": "jsonl", "path": "pnl.jsonl", "follow": true },
    { "name": "replay", "type": "jsonl-mux", "path": "-", "lineDelayMs": 0 }   // lines: {"source","record","timestamp"?}
  ],
  "sink": { "type": "jsonl", "path": "-" },
  "streams": [ /* see ConfigStreams javadoc */ ]
}
```

**Stream fields.** `name`, `source`, optional `timestamp` (epoch ms or ISO; falls back to the source
timestamp, then ingest time), optional `steps`, `key` (`scope`, optional `date`, which is derived from event
time in `zone` when absent, and `attributes`), and `subjects`. Set `normalize: false` on a subject to keep the
raw string.

**Expressions.** A field path `"a.b.0.c"`, `{"field": p, "default": v}`, `{"const": v}`, `{"min": [..]}`,
`{"max": [..]}` or `{"coalesce": [..]}`.

**Steps.**
- `{"filter": predicate}`: a predicate is `{"all"|"any": [..]}`, `{"not": p}`, or `{"field": expr, "op": "eq|ne|in|notIn|containsAny|lt|lte|gt|gte|exists|missing", "value"|"values": ..}`.
- `{"explode": {"path", "as"}}`: emits one record per entry, with `as.key`/`as.index` and `as.value` added.
- `{"unwrap": path}`: replaces the record with a nested object.

**Rules.** `id`, `version`, `subject`, `scope`, and `match` (attribute → value or list of values; omitted
attributes are wildcards). Thresholds are `fire {type, operator, value, expiryMs}` (expiry `-1` means
none) and an optional `resolve {type, operator, value, minTimeToResolveMs}`. Also `severity` and `active`.
Types are `ABSOLUTE`, `RELATIVE` and `RATIO`; the `THRESHOLD_TYPE_` prefix is accepted. Operators are
`LT`, `GT`, `LTE` and `GTE`; `OPERATOR_LESS_THAN` and the like are accepted. Invalid rules are logged and
skipped.

## Behavior preserved from the spec

- Rule algorithm, exactly as in section 6. That covers dedupe against the fire-time value, breach
  carry-over, min-time-to-resolve, expiry, keep-ticking, and the precedence resolved → expired → fired.
  Division is scale 18 with `HALF_UP`.
- Throttling: one evaluation per key per `outputInterval`, using the latest event, stamped with the first
  event's time when `timeBasis=EVENT`.
- End-of-day expiry in `zone`, which clears the key. Late events start a fresh cycle.
- Alert id format `<base64url(sha256[0..n])>:<created_at>`.

## Deliberate differences (spec tech-debt items addressed)

| Spec item | Here |
|---|---|
| K1 duplicated matching | Matching lives only in `RuleMatcher`. |
| K2 duplicate/unstable step names | Streams are named explicitly; duplicate names are rejected. |
| K3 state keyed by full `Rule` proto | State is keyed by rule **id**. Editing a rule updates the active alert (same id, new version). |
| K3/K19 deleted, deactivated or re-scoped rules orphan alerts until EOD | Emits `RESOLVED_RULE_REMOVED` on the key's next evaluation (`resolveOnRuleRemoval=false` restores the old behavior). Inactive rules are filtered client-side. |
| K4 FIRED_UPDATED never written back | Opt-in `dedupeAgainstLastEmitted`. The default keeps the spec semantics. |
| K5 confusing timestamps | `timeBasis` is selectable. EOD `updated_at` is the engine clock, not the fire burst's time. |
| K6 rules API failure empties the catalog | The last good catalog is kept. |
| K7 INVALID-as-wildcard contract | The provider fetches everything explicitly; "wildcard" means the attribute is omitted from `match`. |
| K8 netting dropped and not matchable | Any key attribute is matchable. The risk example copies `netting_id` onto liquidation keys. |
| K9 unbounded state | Keys with no active alert hold no state after evaluation; EOD drops the key entirely. |
| K11/K12/K17 config drift and bloat | One config file, no framework. |
| K13/K14 timer and Spring misuse | Plain scheduler timers; the id generator is constructed directly. |
| K15 id hashes protobuf `toString` | Hashes a portable canonical string. It also includes the firing clock, so two separate fires with the same event time get different ids. The spec's formula collides on E6 vs E12. |
| K18 `View.asSingleton` multi-element pane failure | Refresh runs on a fixed delay, so there's no backlog. |
| K20 count-only integration test | `RiskScenarioTest` asserts states and rule ids per step, plus ids, values and timestamps. |
| Poison rule/event retried forever | A bad rule is rejected at parse time. A bad event fails only that evaluation (`evaluation_errors` metric). |

## Not included (extension points)

- **Kafka/Kinesis connectors.** Implement `Source` / `AlertSink`, or call `engine.submit` from your
  consumer. With no dependencies there is no Kafka client here.
- **Durable state.** State is in memory, so a restart forgets active alerts, as in the original with
  `SKIP_RESTORE_FROM_SNAPSHOT`. For exactly-once or durable state, run the same `Shard` logic inside
  Flink/Beam keyed state, or add snapshotting of `KeyState.activeAlerts`.
- **Sink failures** are logged and counted (`sink_errors`), not retried.
