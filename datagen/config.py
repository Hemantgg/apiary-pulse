"""Simulator settings and Confluent credentials (from credentials.env)."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent

READINGS_TOPIC = os.environ.get("READINGS_TOPIC", "hive_readings")
REGISTRY_TOPIC = os.environ.get("REGISTRY_TOPIC", "apiary_registry")

INTERVAL_SEC = float(os.environ.get("SIM_INTERVAL_SEC", "8"))
LOOP = os.environ.get("SIM_LOOP", "true").lower() == "true"
SCENARIO = os.environ.get("SIM_SCENARIO", "normal").lower()


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap: str
    api_key: str
    api_secret: str
    sr_url: str
    sr_api_key: str
    sr_api_secret: str


def _pick(env: dict[str, str], *keys: str) -> str:
    for key in keys:
        val = env.get(key)
        if val:
            return val
    return ""


def kafka_settings() -> KafkaSettings:
    file_vals = {k: v for k, v in dotenv_values(PROJECT_ROOT / "credentials.env").items() if v}
    from_file = {k.lower(): v for k, v in file_vals.items()}
    from_os = {k.lower(): v for k, v in os.environ.items() if v}
    env = {**from_file, **from_os}

    bootstrap = _pick(env, "kafka_bootstrap", "kafka_bootstrap_servers", "bootstrap_servers")
    api_key = _pick(env, "kafka_api_key")
    api_secret = _pick(env, "kafka_api_secret")
    sr_url = _pick(env, "sr_url", "schema_registry_url")
    sr_key = _pick(env, "sr_api_key", "schema_registry_api_key")
    sr_secret = _pick(env, "sr_api_secret", "schema_registry_api_secret")

    missing = [
        name
        for name, val in [
            ("KAFKA_BOOTSTRAP", bootstrap),
            ("KAFKA_API_KEY", api_key),
            ("KAFKA_API_SECRET", api_secret),
            ("SR_URL", sr_url),
            ("SR_API_KEY", sr_key),
            ("SR_API_SECRET", sr_secret),
        ]
        if not val
    ]

    if missing:
        print(
            "Missing credentials: " + ", ".join(missing) + "\n"
            "Copy credentials.env.example → credentials.env and fill values.\n"
            "See README.md and credentials.env.example.",
            file=sys.stderr,
        )
        sys.exit(1)

    return KafkaSettings(bootstrap, api_key, api_secret, sr_url, sr_key, sr_secret)
