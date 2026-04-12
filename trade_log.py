"""
trade_log.py  —  Structured trade journal with PnL and portfolio tracking
──────────────────────────────────────────────────────────────────────────
Writes to logs/trade_log.json  (append-only trade records + running totals).

PnL is computed using:
  1. Actual broker fill prices (fetched via Upstox order API) — preferred
  2. yfinance spot-price of the last bar at trade time — fallback

Because we trade NSE futures via MARKET orders, the spot close price at
signal time is a reasonable proxy for the fill price when the broker fill
isn't available.

Trade schema
─────────────
{
  "id":              "T0001",
  "status":          "open" | "closed",
  "direction":       "long" | "short",
  "entry_timestamp": "YYYY-MM-DD HH:MM:SS IST",
  "entry_z":         float,
  "beta":            float,
  "ou_mean":         float | null,
  "total_lots":      int,
  "lot_long":        int,    # lot size of T1 futures contract
  "lot_short":       int,    # lot size of T2 futures contract
  "t1_name":         str,
  "t2_name":         str,
  "legs": [
    {
      "event":         "entry" | "pyramid" | "exit",
      "timestamp":     "YYYY-MM-DD HH:MM:SS IST",
      "lots":          int,
      "z_score":       float,
      "t1_spot_price": float | null,   # yfinance last-bar close
      "t2_spot_price": float | null,
      "t1_fill_price": float | null,   # broker confirmed avg fill
      "t2_fill_price": float | null,
      "order_ids":     {"long_leg": "...", "short_leg": "..."}
    },
    ...
  ],
  "exit_timestamp":  str | null,
  "exit_z":          float | null,
  "exit_reason":     str | null,
  "pnl": {
    "t1_entry_avg":  float,
    "t2_entry_avg":  float,
    "t1_exit_price": float,
    "t2_exit_price": float,
    "t1_pnl":        float,
    "t2_pnl":        float,
    "total_pnl":     float,
    "total_lots":    int,
    "used_fills":    bool   # True if broker fills were used
  } | null
}
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
logger = logging.getLogger(__name__)


class TradeLogger:
    def __init__(self, log_file: str):
        self.log_file = log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self._data = self._load()

    # ── Persistence ───────────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"[TRADE LOG] File corrupt, starting fresh: {e}")
        return {
            "trades": [],
            "portfolio": {
                "total_pnl": 0.0,
                "closed_trades": 0,
                "wins": 0,
                "losses": 0,
            },
        }

    def _save(self):
        with open(self.log_file, "w") as f:
            json.dump(self._data, f, indent=2, default=str)

    # ── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _now_str() -> str:
        return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    def _next_trade_id(self) -> str:
        return f"T{len(self._data['trades']) + 1:04d}"

    def _get_open_trade(self) -> Optional[dict]:
        for trade in reversed(self._data["trades"]):
            if trade["status"] == "open":
                return trade
        return None

    @staticmethod
    def _r(val, digits=4) -> Optional[float]:
        """Safe round — returns None if val is None/falsy-zero."""
        if val is None:
            return None
        try:
            return round(float(val), digits)
        except (TypeError, ValueError):
            return None

    # ── Public interface ──────────────────────────────────────────

    def log_entry(
        self,
        direction: str,
        lots: int,
        z_score: float,
        beta: float,
        ou_mean: Optional[float],
        order_ids: dict,
        t1_name: str,
        t2_name: str,
        lot_long: int,
        lot_short: int,
        t1_spot: Optional[float] = None,
        t2_spot: Optional[float] = None,
        t1_fill: Optional[float] = None,
        t2_fill: Optional[float] = None,
    ):
        """Record a new trade entry (initial lot)."""
        trade = {
            "id": self._next_trade_id(),
            "status": "open",
            "direction": direction,
            "entry_timestamp": self._now_str(),
            "entry_z": self._r(z_score),
            "beta": self._r(beta, 6),
            "ou_mean": self._r(ou_mean, 6),
            "total_lots": lots,
            "lot_long": lot_long,
            "lot_short": lot_short,
            "t1_name": t1_name,
            "t2_name": t2_name,
            "legs": [
                {
                    "event": "entry",
                    "timestamp": self._now_str(),
                    "lots": lots,
                    "z_score": self._r(z_score),
                    "t1_spot_price": self._r(t1_spot),
                    "t2_spot_price": self._r(t2_spot),
                    "t1_fill_price": self._r(t1_fill),
                    "t2_fill_price": self._r(t2_fill),
                    "order_ids": order_ids,
                }
            ],
            "exit_timestamp": None,
            "exit_z": None,
            "exit_reason": None,
            "pnl": None,
        }
        self._data["trades"].append(trade)
        self._save()
        logger.info(
            f"[TRADE LOG] Entry  {trade['id']}  {direction.upper()}  {lots} lot(s)  "
            f"z={z_score:.3f}  T1={t1_spot or t1_fill or '?'}  T2={t2_spot or t2_fill or '?'}"
        )

    def log_pyramid(
        self,
        lots_added: int,
        z_score: float,
        order_ids: dict,
        t1_spot: Optional[float] = None,
        t2_spot: Optional[float] = None,
        t1_fill: Optional[float] = None,
        t2_fill: Optional[float] = None,
    ):
        """Record an add-on (pyramid) leg to the current open trade."""
        trade = self._get_open_trade()
        if trade is None:
            logger.warning("[TRADE LOG] log_pyramid called but no open trade found — skipping.")
            return
        trade["total_lots"] += lots_added
        trade["legs"].append(
            {
                "event": "pyramid",
                "timestamp": self._now_str(),
                "lots": lots_added,
                "z_score": self._r(z_score),
                "t1_spot_price": self._r(t1_spot),
                "t2_spot_price": self._r(t2_spot),
                "t1_fill_price": self._r(t1_fill),
                "t2_fill_price": self._r(t2_fill),
                "order_ids": order_ids,
            }
        )
        self._save()
        logger.info(
            f"[TRADE LOG] Pyramid  {trade['id']}  +{lots_added} lot(s)  "
            f"z={z_score:.3f}  total_lots={trade['total_lots']}"
        )

    def log_exit(
        self,
        reason: str,
        z_score: float,
        order_ids: dict,
        t1_spot: Optional[float] = None,
        t2_spot: Optional[float] = None,
        t1_fill: Optional[float] = None,
        t2_fill: Optional[float] = None,
    ):
        """Record exit, compute PnL, and update portfolio totals."""
        trade = self._get_open_trade()
        if trade is None:
            logger.warning("[TRADE LOG] log_exit called but no open trade found — skipping.")
            return

        exit_leg = {
            "event": "exit",
            "timestamp": self._now_str(),
            "lots": trade["total_lots"],
            "z_score": self._r(z_score),
            "t1_spot_price": self._r(t1_spot),
            "t2_spot_price": self._r(t2_spot),
            "t1_fill_price": self._r(t1_fill),
            "t2_fill_price": self._r(t2_fill),
            "order_ids": order_ids,
            "reason": reason,
        }
        trade["legs"].append(exit_leg)
        trade["status"] = "closed"
        trade["exit_timestamp"] = self._now_str()
        trade["exit_z"] = self._r(z_score)
        trade["exit_reason"] = reason

        # Compute PnL
        pnl = self._calc_pnl(trade, t1_spot, t2_spot, t1_fill, t2_fill)
        trade["pnl"] = pnl

        # Update running portfolio totals
        portfolio = self._data["portfolio"]
        portfolio["closed_trades"] += 1
        if pnl and pnl.get("total_pnl") is not None:
            portfolio["total_pnl"] = round(portfolio["total_pnl"] + pnl["total_pnl"], 2)
            if pnl["total_pnl"] > 0:
                portfolio["wins"] += 1
            else:
                portfolio["losses"] += 1

        self._save()

        pnl_str = (
            f"PnL={pnl['total_pnl']:+.2f}"
            if pnl and pnl.get("total_pnl") is not None
            else "PnL=N/A (prices unavailable)"
        )
        logger.info(
            f"[TRADE LOG] Exit  {trade['id']}  reason={reason}  "
            f"z={z_score:.3f}  {pnl_str}  "
            f"RunningPnL={portfolio['total_pnl']:+.2f}"
        )

    # ── PnL calculation ───────────────────────────────────────────

    def _calc_pnl(
        self,
        trade: dict,
        t1_exit_spot: Optional[float],
        t2_exit_spot: Optional[float],
        t1_exit_fill: Optional[float],
        t2_exit_fill: Optional[float],
    ) -> Optional[dict]:
        """
        Compute spread PnL using fill prices when available, else spot.

        Long spread  (BUY T1 / SELL T2):
            PnL = (exit_T1 - entry_T1) * qty_T1 + (entry_T2 - exit_T2) * qty_T2

        Short spread (SELL T1 / BUY T2):
            PnL = (entry_T1 - exit_T1) * qty_T1 + (exit_T2 - entry_T2) * qty_T2
        """
        # Resolve exit prices (fill > spot)
        t1_exit = t1_exit_fill or t1_exit_spot
        t2_exit = t2_exit_fill or t2_exit_spot
        used_fills = bool(t1_exit_fill or t2_exit_fill)

        if t1_exit is None or t2_exit is None:
            return {"error": "exit prices unavailable", "total_pnl": None}

        # Build weighted-average entry prices from entry + pyramid legs
        t1_sum = t2_sum = total_lots = 0
        for leg in trade["legs"]:
            if leg["event"] not in ("entry", "pyramid"):
                continue
            t1_p = leg.get("t1_fill_price") or leg.get("t1_spot_price")
            t2_p = leg.get("t2_fill_price") or leg.get("t2_spot_price")
            if t1_p is None or t2_p is None:
                continue
            n = leg["lots"]
            t1_sum  += t1_p * n
            t2_sum  += t2_p * n
            total_lots += n

        if total_lots == 0:
            return {"error": "entry prices unavailable", "total_pnl": None}

        t1_entry_avg = t1_sum / total_lots
        t2_entry_avg = t2_sum / total_lots

        lot_long  = trade["lot_long"]
        lot_short = trade["lot_short"]
        qty_long  = total_lots * lot_long
        qty_short = total_lots * lot_short

        if trade["direction"] == "long":
            t1_pnl = (t1_exit - t1_entry_avg) * qty_long
            t2_pnl = (t2_entry_avg - t2_exit)  * qty_short
        else:  # short
            t1_pnl = (t1_entry_avg - t1_exit) * qty_long
            t2_pnl = (t2_exit - t2_entry_avg)  * qty_short

        total_pnl = t1_pnl + t2_pnl

        return {
            "t1_entry_avg":  round(t1_entry_avg, 4),
            "t2_entry_avg":  round(t2_entry_avg, 4),
            "t1_exit_price": round(t1_exit, 4),
            "t2_exit_price": round(t2_exit, 4),
            "t1_pnl":        round(t1_pnl, 2),
            "t2_pnl":        round(t2_pnl, 2),
            "total_pnl":     round(total_pnl, 2),
            "total_lots":    total_lots,
            "used_fills":    used_fills,
        }

    # ── Portfolio summary ─────────────────────────────────────────

    def get_portfolio_summary(self) -> dict:
        """Return a dict of running portfolio statistics."""
        p = self._data["portfolio"]
        closed = p["closed_trades"]
        wins   = p["wins"]

        open_trade = self._get_open_trade()

        summary = {
            "total_trades":   closed,
            "open_trade_id":  open_trade["id"] if open_trade else None,
            "open_direction": open_trade["direction"] if open_trade else None,
            "open_since":     open_trade["entry_timestamp"] if open_trade else None,
            "open_lots":      open_trade["total_lots"] if open_trade else None,
            "wins":           wins,
            "losses":         p["losses"],
            "win_rate":       round(wins / closed, 3) if closed > 0 else None,
            "total_pnl":      p["total_pnl"],
        }

        # Per-trade PnL series for drawdown / best / worst
        closed_pnls = [
            t["pnl"]["total_pnl"]
            for t in self._data["trades"]
            if t["status"] == "closed"
            and t.get("pnl")
            and t["pnl"].get("total_pnl") is not None
        ]
        if closed_pnls:
            summary["best_trade"]  = round(max(closed_pnls), 2)
            summary["worst_trade"] = round(min(closed_pnls), 2)
            summary["avg_trade"]   = round(sum(closed_pnls) / len(closed_pnls), 2)
            # Running drawdown
            running = peak = max_dd = 0.0
            for pnl in closed_pnls:
                running += pnl
                if running > peak:
                    peak = running
                dd = running - peak
                if dd < max_dd:
                    max_dd = dd
            summary["max_drawdown"] = round(max_dd, 2)

        return summary

    def log_portfolio_summary(self):
        """Emit a one-line INFO log with current portfolio stats."""
        s = self.get_portfolio_summary()
        wr = f"{s['win_rate']:.1%}" if s["win_rate"] is not None else "N/A"
        open_info = (
            f"{s['open_trade_id']} {s['open_direction'].upper()} "
            f"{s['open_lots']}lot(s) since {s['open_since']}"
            if s["open_trade_id"]
            else "Flat"
        )
        logger.info(
            f"[PORTFOLIO] "
            f"Trades={s['total_trades']}  W/L={s['wins']}/{s['losses']}  "
            f"WinRate={wr}  TotalPnL={s['total_pnl']:+.2f}  "
            f"MaxDD={s.get('max_drawdown', 0.0):+.2f}  "
            f"Position={open_info}"
        )
