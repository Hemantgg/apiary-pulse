"""Publish governed Avro hive readings to Confluent Cloud."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from datagen import config
from datagen.scenarios import initial_hives

from datagen.config import kafka_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _producer() -> tuple[Producer, AvroSerializer]:
    ks = kafka_settings()
    sr = SchemaRegistryClient(
        {
            "url": ks.sr_url,
            "basic.auth.user.info": f"{ks.sr_api_key}:{ks.sr_api_secret}",
        }
    )
    serializer = AvroSerializer(
        sr,
        schema_str=None,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    producer = Producer(
        {
            "bootstrap.servers": ks.bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": ks.api_key,
            "sasl.password": ks.api_secret,
        }
    )
    return producer, serializer


def _reading_payload(hive, observed_at_ms: int) -> dict:
    return {
        "reading_id": str(uuid.uuid4()),
        "hive_id": hive.hive_id,
        "apiary_id": hive.apiary_id,
        "gateway_id": hive.gateway_id,
        "observed_at": observed_at_ms,
        "weight_kg": round(hive.weight_kg, 2),
        "temp_c": round(hive.temp_c, 2),
        "humidity_pct": round(hive.humidity_pct, 1),
        "activity_count": hive.activity,
    }


def _bad_reading_payload(hive) -> dict:
    """Schema-valid but fails Flink validation (DLQ demo)."""
    payload = _reading_payload(hive, _now_millis())
    payload["weight_kg"] = 250.0
    payload["reading_id"] = f"bad-{uuid.uuid4()}"
    return payload


def run() -> None:
    producer, avro = _producer()
    hives = initial_hives()
    ctx = SerializationContext(config.READINGS_TOPIC, MessageField.VALUE)
    tick = 0

    logger.info(
        "Publishing to topic=%s every %ss scenario=%s (Ctrl+C to stop)",
        config.READINGS_TOPIC,
        config.INTERVAL_SEC,
        config.SCENARIO,
    )

    try:
        while True:
            for hive in hives:
                hive.tick(config.SCENARIO)
                if config.SCENARIO == "bad_reading" and tick % 25 == 0:
                    payload = _bad_reading_payload(hive)
                else:
                    payload = _reading_payload(hive, _now_millis())
                producer.produce(
                    topic=config.READINGS_TOPIC,
                    key=payload["hive_id"].encode("utf-8"),
                    value=avro(payload, ctx),
                )
            producer.flush()
            tick += 1
            if not config.LOOP and tick >= 120:
                break
            time.sleep(config.INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    run()
