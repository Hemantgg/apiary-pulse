"""Per-hive state for realistic weight / climate drift."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class HiveState:
    hive_id: str
    apiary_id: str
    gateway_id: str
    weight_kg: float
    temp_c: float
    humidity_pct: float
    activity: int = 40
    _swarm_armed: bool = field(default=False, repr=False)

    def tick(self, scenario: str) -> None:
        self.weight_kg += random.uniform(-0.15, 0.12)
        self.temp_c += random.uniform(-0.08, 0.08)
        self.humidity_pct += random.uniform(-0.6, 0.6)
        self.activity = max(0, int(self.activity + random.randint(-6, 8)))

        self.temp_c = max(28.0, min(38.0, self.temp_c))
        self.humidity_pct = max(35.0, min(75.0, self.humidity_pct))

        if scenario == "swarm" and self.hive_id == "HIVE-ALPHA" and not self._swarm_armed:
            if random.random() < 0.04:
                self._swarm_armed = True
        if self._swarm_armed:
            self.weight_kg -= random.uniform(2.5, 5.0)
            self.activity += random.randint(15, 35)

        if scenario == "theft" and self.hive_id == "HIVE-BETA":
            self.weight_kg -= random.uniform(0.4, 1.2)


def initial_hives() -> list[HiveState]:
    return [
        HiveState("HIVE-ALPHA", "APIARY-BKK-01", "gw-bkk-01", 42.5, 34.2, 55.0),
        HiveState("HIVE-BETA", "APIARY-BKK-01", "gw-bkk-02", 39.0, 33.8, 52.0),
        HiveState("HIVE-GAMMA", "APIARY-CNX-02", "gw-cnx-01", 48.2, 34.5, 58.0),
    ]
