# Self-hosted deployment (Docker Compose / VMs)

The alerting job is an Apache Beam pipeline that runs on a self-hosted **Flink 1.19 (Java 17)** cluster.
Checkpoints and savepoints go to **Garage**, a self-hosted S3-compatible object store. This directory holds
a complete local stack plus the scripts you need to operate it.

| Piece | What it is |
|---|---|
| `Dockerfile` | Job image: `flink:1.19-java17`, the S3 filesystem plugin, the job fat jar and the example configs. JobManager, TaskManagers and the `flink run` client all use it. |
| `docker-compose.yml` | Kafka (KRaft, single node), Garage (single node) and a Flink session cluster: 1 JobManager and N TaskManagers. |
| `garage/garage.toml` | Single-node Garage for development. |
| `scripts/init-env.sh` | Writes `deploy/.env` with random Garage secrets. The file is gitignored. |
| `scripts/garage-init.sh` | One-time Garage setup: layout, bucket and access key. Safe to re-run. |
| `scripts/submit.sh` | Submits the job: `flink run -d ... --config=... --checkpointingInterval=...`. |
| `scripts/upgrade.sh` | Redeploys without losing state: stops with a savepoint, then resubmits from it. |
| `scripts/replay.sh` | Produces an example `scenario.jsonl` into Kafka, for demos and smoke tests. |

## Quick start (local)

```bash
./gradlew shadowJar
./deploy/scripts/init-env.sh
docker compose -f deploy/docker-compose.yml up -d --build
./deploy/scripts/garage-init.sh
OFFSET_RESET=earliest ./deploy/scripts/submit.sh
./deploy/scripts/replay.sh examples/risk
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic luna.risk_alert.events --from-beginning
```

The Flink UI is at http://localhost:8081. To run another domain, point the job at that domain's config:
`./deploy/scripts/submit.sh --config /opt/flink/usrlib/examples/security/pipeline.json`. Several jobs (one per
domain) can share the cluster as long as there are enough task slots.

## Running against existing infrastructure

The compose file reads everything environment-specific from `deploy/.env` or the shell:

| Variable | Used by | Meaning |
|---|---|---|
| `S3_ENDPOINT`, `S3_REGION` | Flink | Your Garage S3 API, e.g. `http://garage.internal:3900` with region `garage` (the `s3_region` set in garage.toml). |
| `GARAGE_BUCKET`, `GARAGE_ACCESS_KEY`, `GARAGE_SECRET_KEY` | Flink | Bucket and key for checkpoints and savepoints. The key needs read and write on the bucket. |
| `KAFKA_BOOTSTRAP` | submit.sh | Brokers, substituted into `${KAFKA_BOOTSTRAP}` in the pipeline config. |
| `OFFSET_RESET` | submit.sh | `latest` (spec default) or `earliest`, used when a consumer group has no committed offsets. |
| `OUTPUT_INTERVAL_MS` | submit.sh | Per-key throttle (spec default 1000). |
| `CHECKPOINT_INTERVAL_MS` | submit.sh | Checkpoint interval (default 60000). |
| `PARALLELISM`, `TASK_SLOTS`, `TASKMANAGER_MEMORY` | Flink | Sizing. |

To use your own Garage cluster, create the bucket and key there:
`garage bucket create flink`, `garage key create flink`, then
`garage bucket allow --read --write flink --key flink`. Put the values in `deploy/.env`, and drop the
`garage` service, or simply don't run `garage-init.sh`.

On plain VMs, run the same image with `jobmanager` / `taskmanager` as the command and the same
`FLINK_PROPERTIES`, or put those keys into each host's `conf/config.yaml`. The scripts only need to reach
the JobManager.

## Upgrades keep state

AWS Managed Flink in the original setup skipped snapshot restore on every deploy, so all active alerts were
lost (spec K10). Here:

```bash
./gradlew shadowJar && docker compose -f deploy/docker-compose.yml up -d --build
./deploy/scripts/upgrade.sh
```

`upgrade.sh` runs `flink stop` (which takes a savepoint into Garage), then resubmits with `-s <savepoint>`.
Active alerts, in-flight throttle bursts, end-of-day timers and Kafka offsets all carry over. Operator state
is matched by transform name, so **don't rename streams or sources** in a config you want to restore. Add new
ones instead.

## Notes

- Flink 1.19 stores its config as nested YAML, and `s3.endpoint.region` is silently dropped next to
  `s3.endpoint`. The compose file therefore sets the Hadoop key `fs.s3a.endpoint.region`.
- Garage has no directory markers, so the S3A client logs harmless `MultiObjectDeleteSupport ... NoSuchKey`
  warnings when it cleans them up.
- The job jar excludes Flink classes, because the cluster provides them. Beam's `META-INF/services` files are
  merged at build time; without that the job fails at submit with "Model coder not registered".
- `submit.sh` passes `--attachedMode=false`. With `flink run -d`, Beam must not wait for job results.
