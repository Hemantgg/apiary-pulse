#!/usr/bin/env python3
"""Entry point: uv run python scripts/run_simulator.py"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datagen.simulator import run

if __name__ == "__main__":
    run()
