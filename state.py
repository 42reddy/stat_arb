"""
state.py  —  Persistent position tracker
─────────────────────────────────────────
Stores the current open position (if any) to a JSON file.
This means the bot can be restarted without forgetting it
has an open trade.

Position schema:
{
    "direction":   "long" | "short" | null,
    "lots":        int,                      # number of lots currently held
    "entry_date":  "YYYY-MM-DD",
    "entry_z":     float,
    "beta":        float,
    "ou_mean":     float,
    "order_ids":   {"long_leg": "...", "short_leg": "..."}
}
"""

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class PositionState:
    def __init__(self, state_file: str):
        self.state_file = state_file
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        self._state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f:
                    s = json.load(f)
                logger.info(f"Loaded position state: {s}")
                return s
            except Exception as e:
                logger.warning(f"State file corrupt, resetting: {e}")
        return self._empty()

    def _empty(self) -> dict:
        return {
            "direction": None,
            "lots": 0,
            "entry_date": None,
            "entry_z": None,
            "beta": None,
            "ou_mean": None,
            "order_ids": {},
        }

    def _save(self):
        with open(self.state_file, "w") as f:
            json.dump(self._state, f, indent=2)

    # ── Public interface ─────────────────────────────────────────

    @property
    def is_flat(self) -> bool:
        return self._state["direction"] is None

    @property
    def direction(self) -> Optional[str]:
        return self._state["direction"]

    @property
    def lots(self) -> int:
        return self._state["lots"]

    @property
    def entry_date(self) -> Optional[str]:
        return self._state["entry_date"]

    @property
    def entry_z(self) -> Optional[float]:
        return self._state["entry_z"]

    @property
    def beta(self) -> Optional[float]:
        return self._state["beta"]

    @property
    def ou_mean(self) -> Optional[float]:
        return self._state["ou_mean"]

    def open_position(
        self,
        direction: str,
        lots: int,
        entry_date: str,
        entry_z: float,
        beta: float,
        ou_mean: float,
        order_ids: dict,
    ):
        self._state = {
            "direction": direction,
            "lots": lots,
            "entry_date": entry_date,
            "entry_z": entry_z,
            "beta": beta,
            "ou_mean": ou_mean,
            "order_ids": order_ids,
        }
        self._save()
        logger.info(f"Position opened: {self._state}")

    def add_lots(self, extra_lots: int, order_ids: dict):
        self._state["lots"] += extra_lots
        self._state["order_ids"].update(order_ids)
        self._save()
        logger.info(f"Pyramided: now {self._state['lots']} lots")

    def close_position(self):
        logger.info(f"Closing position: {self._state}")
        self._state = self._empty()
        self._save()
