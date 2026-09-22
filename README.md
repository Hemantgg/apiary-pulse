# ApiaryPulse

Real-time hive health monitoring on **Confluent Cloud**: governed Avro ingest, **Apache Flink** rule-based alerts (no ML), dead-letter handling, and **HTTP Sink** webhook delivery.

```
hive_readings ──► Flink (validated + DLQ) ──► Flink (hive_alerts) ──► HTTP Sink
                      ▲
               apiary_registry
```

## Stack

- Kafka + Schema Registry (Avro)
- Flink SQL (Confluent Cloud)
- Python simulator + optional FastAPI alert receiver

## Setup

1. Create a Confluent Cloud **environment** with a **Kafka cluster**, **Schema Registry**, and **Flink compute pool** (same region).

2. **Credentials** — copy `credentials.env.example` to `credentials.env` and set:
   - `KAFKA_BOOTSTRAP`, `KAFKA_API_KEY`, `KAFKA_API_SECRET` (cluster API key)
   - `SR_URL`, `SR_API_KEY`, `SR_API_SECRET` (Schema Registry API key — separate from Kafka)

   ```bash
   uv sync
   ```

3. **Flink SQL** (Console → Flink → SQL workspace; catalog = environment, database = cluster). Run in order:

   | # | File |
   |---|------|
   | 1 | `flink/01_create_hive_readings.sql` |
   | 2 | `flink/02_create_apiary_registry.sql` |
   | 3 | `flink/03_seed_apiary_registry.sql` |
   | 4 | `flink/04_job_readings_validated.sql` |
   | 5 | `flink/05_job_readings_dlq.sql` |
   | 6 | `flink/06_job_hive_alerts.sql` |

4. **Telemetry**

   ```bash
   uv run python scripts/run_simulator.py
   ```

   `SIM_SCENARIO=swarm` or `bad_reading` for demo variants.

5. **Webhook (HTTP Sink)** — point a Confluent **HTTP Sink** on topic `hive_alerts` at your endpoint:

   ```bash
   uv run uvicorn scripts.alert_receiver:app --port 8787
   ```

## Schemas

Avro definitions: [`schemas/`](schemas/) (`hive_reading`, `hive_alert`, `apiary_registry`).

## License

MIT
