# real-time-alerts

A domain-agnostic streaming alert pipeline built on **Apache Beam** and running on a **self-hosted Apache
Flink 1.19** cluster (Java 17). It follows the `common-alerting` rebuild spec (sources → keyed rule
evaluation → alert state machine → Kafka), generalized so that any event domain is onboarded with
configuration alone: finance, security, operations, and so on.

```
GenerateSequence(every refresh) ─▶ Fetch rules ─▶ newest per pane ─▶ View.asSingleton ─────┐ side input
Kafka topic ─▶ Decode (json | protobuf) ─┬─▶ [stream] Prepare: filter/explode/key ─▶ AlertingDoFn ─┐
                                         └─▶ [stream] Prepare ─▶ AlertingDoFn ─────────────────────┤
Kafka topic ─▶ Decode ───────────────────▶ [stream] Prepare ─▶ AlertingDoFn ─────────────────────┴─▶ Flatten ─▶ Kafka alerts
                                                               (keyed state + processing-time timers)
```

Both [`examples/risk`](examples/risk) (the spec's 7-branch finance pipeline) and
[`examples/security`](examples/security) (failed-login ratios per user, data egress per host) run on the same
jar. Only the config differs.

## How a domain is described

A pipeline config ([PipelineConfig](src/main/java/io/alerting/config/PipelineConfig.java)) has these parts:

- **sources**: Kafka topics, plus how to decode them: `json`, or `protobuf` with a descriptor set and a
  message type, decoded dynamically, so no generated classes are needed.
- **streams**: per alert family, which source to read, then the `steps` (`filter`, `explode`, `unwrap`),
  the `key` (scope, business date, attributes) and the `subjects` (metric value and comparison value).
  Several streams may read one source; each stream keeps independent alert state.
- **rules**: where rules come from: `file`, a paginated `http` API, or `inline`. They are refreshed on an
  interval and broadcast as a side input.
- **engine**: throttle interval, keep-ticking, business-day time zone, time basis, and so on.
- **kafka** / **sink**: brokers, consumer and producer overrides, and the alerts topic.

`${VAR}` and `${VAR:-default}` are resolved from the environment when the job is submitted.

A security stream, for example:

```json
{
  "name": "auth-user", "source": "auth-events", "timestamp": "window_end",
  "steps": [ { "filter": { "field": "user", "op": "exists" } } ],
  "key": { "scope": "USER", "attributes": { "tenant": "tenant", "user": "user", "idp": "idp" } },
  "subjects": {
    "FAILED_LOGIN_RATIO": { "subject": { "name": "failures", "value": "counts.failures" },
                            "compare": { "name": "attempts", "value": "counts.attempts" } }
  }
}
```

And a rule for it:

```json
{ "id": "SEC-1", "subject": "FAILED_LOGIN_RATIO", "scope": "USER", "severity": "HIGH",
  "fire":    { "type": "RATIO", "operator": "GT", "value": "0.5", "expiryMs": 3600000 },
  "resolve": { "type": "RATIO", "operator": "LT", "value": "0.2", "minTimeToResolveMs": 300000 } }
```

**Rules.** `match` maps key attributes to an allowed value or list of values; an attribute that is left out
is a wildcard. Threshold types are `ABSOLUTE`, `RELATIVE` (`(v - c) / |c|`) and `RATIO` (`v / |c|`), with
scale 18 and `HALF_UP` rounding. Operators are `LT`, `GT`, `LTE` and `GTE`. The proto enum names
(`THRESHOLD_TYPE_RATIO`, `OPERATOR_LESS_THAN`) are accepted too. Invalid rules are logged and skipped.

**Expressions.** A value can be a field path `"a.b.0.c"`, `{"field", "default"}`, `{"const"}`, `{"min": [..]}`,
`{"max": [..]}` or `{"coalesce": [..]}`.

**Predicates.** `{"all"|"any": [..]}`, `{"not"}`, or `{"field", "op": eq|ne|in|notIn|containsAny|lt|lte|gt|gte|exists|missing}`.

## Alert lifecycle

Alerts are kept per (stream, key, rule id):
[AlertStateMachine](src/main/java/io/alerting/core/AlertStateMachine.java), run by
[AlertingDoFn](src/main/java/io/alerting/pipeline/AlertingDoFn.java).

| Active? | Evaluation | Emits |
|---|---|---|
| no | fire threshold met | `FIRED` (new id) and arms the end-of-day timer |
| yes | resolve threshold met, after `minTimeToResolveMs` | `RESOLVED_AUTOMATICALLY` |
| yes | not breaching and `expiryMs` elapsed | `RESOLVED_EXPIRED` |
| yes | breaching, or `keepTicking` | `FIRED_UPDATED` (same id, breach value carried) |
| yes | value unchanged since fire | nothing (dedupe) |
| yes | rule deleted, deactivated or no longer matching | `RESOLVED_RULE_REMOVED` |
| yes | end of business day in `zone` | `RESOLVED_EXPIRED` for every alert on the key |

Each key is evaluated at most once per `outputIntervalMs`, using the latest event (throttle). Output goes to
Kafka keyed by `{"alert_id","date"}`, so all transitions of one alert land on one partition in order.

## Build, test, run

```bash
./gradlew test
```

```bash
./gradlew shadowJar
```

`./gradlew test` covers the rule engine, the state machine, config compilation, coders, protobuf decoding
and the rules side input. It also replays both example scenarios through the real Beam graph on the
DirectRunner, with `TestStream` driving processing time: the spec's E1–E17 for finance, plus the security
scenario. `./gradlew shadowJar` builds the job jar, `build/libs/real-time-alerts-<version>-all.jar`.

Deployment to a self-hosted Flink cluster with Kafka and Garage checkpoint storage, plus zero-loss upgrades
via savepoints, is described in [deploy/README.md](deploy/README.md).

## Differences from the spec

| Spec | Here |
|---|---|
| Seven hardcoded branches and evaluator classes (Java + Spring) | Streams are config, and one generic DoFn is reused per stream. No Spring. |
| AWS Managed Flink 1.18, Java 11 | Self-hosted Flink 1.19, Java 17, Docker Compose or VMs. |
| `SKIP_RESTORE_FROM_SNAPSHOT`: active alerts lost every deploy (K10) | `upgrade.sh` stops with a savepoint into Garage and restores from it. |
| Alert state keyed by full `Rule` proto (K3) | Keyed by rule id; editing a rule keeps the alert. |
| Rules API failure empties the side input (K6) | A failed fetch emits nothing, so the last good catalog stays. |
| `View.asSingleton` fails on multi-tick panes (K18) | Panes are reduced to the newest catalog first. |
| Deleted rules orphan alerts until EOD (K3/K19) | `RESOLVED_RULE_REMOVED` (opt-out: `resolveOnRuleRemoval: false`). |
| Per-date keys and rule snapshots kept forever (K9) | A key with no active alert holds no state after evaluation. |
| Netting dropped on liquidation keys (K8) | Any attribute can be carried and matched. |
| Poison rule or event retried forever | Rejected at parse time, or skipped and counted (`evaluation_errors`). |
| Alert id hashes protobuf `toString` (K15) | Hashes a portable canonical string; ids stay unique across re-fires at the same event time. |
| No custom metrics | Beam counters per stream: evaluations, errors, alerts by state, rule refreshes. |

## Layout

| Package | Contents |
|---|---|
| `model`, `json` | Rules, keys, alerts and values, plus a small JSON codec. Domain-neutral. |
| `threshold`, `rules` | Threshold evaluators, the rule algorithm (spec section 6), matching, the catalog and rule providers. |
| `core` | The alert state machine, engine settings, alert ids and business-day helpers. Runner-free. |
| `config` | Pipeline config, compiled streams, expressions and predicates. |
| `pipeline` | Beam: decode, prepare, `AlertingDoFn`, the rules side input, coders, graph assembly and the launcher. |
