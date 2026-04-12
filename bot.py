"""
bot.py  —  Stat-Arb Execution Bot  (NHPC / POWERGRID) — Upstox edition
═══════════════════════════════════════════════════════════════════════
Same logic as before, wired to Upstox instead of Zerodha.

Sandbox vs Live is controlled entirely in config.ini:
    [SANDBOX]
    enabled = true    ← paper trading (safe)
    enabled = false   ← real money

Run:
    python bot.py

Requirements:
    pip install upstox-python-sdk upstox-totp yfinance pyotp pandas numpy statsmodels schedule pytz
"""

import logging
import os
import sys
import time
from datetime import datetime, time as dt_time
from configparser import ConfigParser
from typing import Optional

import schedule
import pytz
from dotenv import load_dotenv

from auth       import get_client
from data       import fetch_spot
from state      import PositionState
from execution  import Executor
from features   import features as Features
from trade_log  import TradeLogger

IST = pytz.timezone("Asia/Kolkata")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG & LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def load_config(path: str = "config.ini") -> ConfigParser:
    cfg = ConfigParser(inline_comment_prefixes=(";", "#"))
    cfg.read(path)
    return cfg


def setup_logging(log_file: str):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ══════════════════════════════════════════════════════════════════════════════
# PARAMS — paste your latest Bayesian consensus here
# ══════════════════════════════════════════════════════════════════════════════

PARAMS = {
    "T1":                 "HAL.NS",
    "T2":                 "BDL.NS",
    "slow_window":        23,
    "medium_window":      34,
    "fast_span":          10,
    "vol_window":         20,
    "z_entry_long":       1.7065,
    "z_entry_short":      1.8698,
    "z_exit_long":        0.1105,
    "z_exit_short":       0.2443,
    "z_stop_long":        2.8,
    "z_stop_short":       2.8,
    "z_add":              2.0,
    "vol_cap":            1.5,
    "max_hold":           25,
    "z_entry":            1.25,
    "z_exit":             0.30,
    "z_stop":             3.0,
    "autocorr_window":    20,
    "autocorr_threshold": 0.1,
    "ou_adapt_span":      252,
}

logger = logging.getLogger("bot")


# ══════════════════════════════════════════════════════════════════════════════
# BOT
# ══════════════════════════════════════════════════════════════════════════════

class StatArbBot:
    def __init__(self, cfg: ConfigParser):
        self.cfg        = cfg
        self.state      = PositionState(cfg["PATHS"]["state_file"])
        self.trade_log  = TradeLogger(cfg["PATHS"]["trade_log_file"])
        self.params     = dict(PARAMS)
        self.params["T1"] = cfg["STRATEGY"]["ticker_long"]
        self.params["T2"] = cfg["STRATEGY"]["ticker_short"]
        self.feat_eng   = Features(t1=self.params["T1"], t2=self.params["T2"])
        self.executor   = None
        self._api_client  = None
        self._ou_mean   = None
        self._beta      = None

    def login(self):
        logger.info("=== Logging in to Upstox ===")
        api_client, _ = get_client(self.cfg)
        self._api_client = api_client
        self.executor    = Executor(api_client, self.cfg)
        logger.info("Login OK")

    def ensure_session(self):
        """Re-authenticate if session has gone stale."""
        import upstox_client
        from upstox_client.rest import ApiException
        if self._api_client is None:
            self.login()
            return

        if self._api_client.configuration.sandbox:
            logger.info("Skipping session validation (Sandbox mode)")
            return

        try:
            upstox_client.UserApi(self._api_client).get_profile(api_version='2.0')
        except ApiException as e:
            logger.warning(f"Session check failed (status={e.status}). Re-logging in.")
            self.login()
        except Exception as e:
            logger.warning(f"Session check failed ({e}). Re-logging in.")
            self.login()

    def run_cycle(self):
        now_ist = datetime.now(IST)
        logger.info(f"── Cycle start  {now_ist.strftime('%Y-%m-%d %H:%M IST')} ──")

        self.ensure_session()

        # 1. Fetch spot data
        try:
            df = fetch_spot(self.cfg)
        except Exception as e:
            logger.error(f"Data fetch failed: {e} — skipping cycle")
            return

        # 2. Build features
        try:
            feat, ou_mean = self.feat_eng.build_features(
                df, self.params,
                ou_mean=self._ou_mean,
                beta=self._beta,
            )
        except Exception as e:
            logger.error(f"Feature build failed: {e} — skipping cycle")
            return

        self._ou_mean = ou_mean
        self._beta    = float(feat["beta"].iloc[-1])

        # 3. Generate signals
        sig    = self.feat_eng.generate_signals(feat, self.params)
        latest = sig.iloc[-1]
        z_now  = float(feat["z_slow"].iloc[-1])

        # Spot prices of the last bar — used as price reference in trade log
        t1_spot = float(df[self.params["T1"]].iloc[-1])
        t2_spot = float(df[self.params["T2"]].iloc[-1])

        logger.info(
            f"z_slow={z_now:.3f}  dir={self.state.direction}  lots={self.state.lots}  "
            f"long_entry={latest['long_entry']}  short_entry={latest['short_entry']}  "
            f"exit={latest['exit_any']}  "
            f"T1={t1_spot:.2f}  T2={t2_spot:.2f}"
        )

        # Portfolio summary at each cycle (visible in bot.log)
        self.trade_log.log_portfolio_summary()

        today_str = now_ist.strftime("%Y-%m-%d")

        # 4. Max hold check
        if not self.state.is_flat and self.state.entry_date:
            entry_dt  = datetime.strptime(self.state.entry_date, "%Y-%m-%d")
            hold_days = (now_ist.replace(tzinfo=None) - entry_dt).days
            if hold_days >= self.params["max_hold"]:
                logger.info(f"MAX HOLD reached ({hold_days}d) — forcing exit")
                self._exit(reason="time_stop", z_score=z_now, t1_spot=t1_spot, t2_spot=t2_spot)
                return

        # 5. Exit logic
        if not self.state.is_flat:
            exit_reason = None
            if self.state.direction == "long"  and latest["exit_stop_long"]:
                logger.info(f"STOP triggered on LONG  z={z_now:.3f}")
                exit_reason = "stop"
            elif self.state.direction == "short" and latest["exit_stop_short"]:
                logger.info(f"STOP triggered on SHORT z={z_now:.3f}")
                exit_reason = "stop"
            elif self.state.direction == "long"  and latest["exit_mean_long"]:
                logger.info(f"Mean-revert exit on LONG  z={z_now:.3f}")
                exit_reason = "mean_revert"
            elif self.state.direction == "short" and latest["exit_mean_short"]:
                logger.info(f"Mean-revert exit on SHORT z={z_now:.3f}")
                exit_reason = "mean_revert"
            if exit_reason:
                self._exit(reason=exit_reason, z_score=z_now, t1_spot=t1_spot, t2_spot=t2_spot)
                return

        # 6. Pyramid logic
        max_lots = int(self.cfg["STRATEGY"]["max_lots"])
        if not self.state.is_flat and self.state.lots < max_lots:
            if self.state.direction == "long"  and latest["long_add"]:
                oids = self.executor.add_long(1)
                if not self._legs_ok(oids):
                    logger.error("LONG add rejected: one or more legs failed. State unchanged.")
                    return
                self.state.add_lots(1, oids)
                # Fetch fills (best effort) then log pyramid
                fills = self.executor.get_leg_fills(oids)
                self.trade_log.log_pyramid(
                    lots_added=1, z_score=z_now, order_ids=oids,
                    t1_spot=t1_spot, t2_spot=t2_spot,
                    t1_fill=fills["long_fill"], t2_fill=fills["short_fill"],
                )
                return
            elif self.state.direction == "short" and latest["short_add"]:
                oids = self.executor.add_short(1)
                if not self._legs_ok(oids):
                    logger.error("SHORT add rejected: one or more legs failed. State unchanged.")
                    return
                self.state.add_lots(1, oids)
                fills = self.executor.get_leg_fills(oids)
                self.trade_log.log_pyramid(
                    lots_added=1, z_score=z_now, order_ids=oids,
                    t1_spot=t1_spot, t2_spot=t2_spot,
                    t1_fill=fills["long_fill"], t2_fill=fills["short_fill"],
                )
                return

        # 7. Entry logic
        if self.state.is_flat:
            if latest["long_entry"]:
                logger.info(f"LONG ENTRY  z={z_now:.3f}")
                oids = self.executor.enter_long(lots=1)
                if not self._legs_ok(oids):
                    logger.error("LONG entry rejected: one or more legs failed. State unchanged.")
                    return
                self.state.open_position(
                    direction="long", lots=1,
                    entry_date=today_str, entry_z=z_now,
                    beta=self._beta, ou_mean=self._ou_mean,
                    order_ids=oids,
                )
                fills = self.executor.get_leg_fills(oids)
                self.trade_log.log_entry(
                    direction="long", lots=1, z_score=z_now,
                    beta=self._beta, ou_mean=self._ou_mean,
                    order_ids=oids,
                    t1_name=self.params["T1"], t2_name=self.params["T2"],
                    lot_long=self.executor.lot_long, lot_short=self.executor.lot_short,
                    t1_spot=t1_spot, t2_spot=t2_spot,
                    t1_fill=fills["long_fill"], t2_fill=fills["short_fill"],
                )
            elif latest["short_entry"]:
                logger.info(f"SHORT ENTRY  z={z_now:.3f}")
                oids = self.executor.enter_short(lots=1)
                if not self._legs_ok(oids):
                    logger.error("SHORT entry rejected: one or more legs failed. State unchanged.")
                    return
                self.state.open_position(
                    direction="short", lots=1,
                    entry_date=today_str, entry_z=z_now,
                    beta=self._beta, ou_mean=self._ou_mean,
                    order_ids=oids,
                )
                fills = self.executor.get_leg_fills(oids)
                self.trade_log.log_entry(
                    direction="short", lots=1, z_score=z_now,
                    beta=self._beta, ou_mean=self._ou_mean,
                    order_ids=oids,
                    t1_name=self.params["T1"], t2_name=self.params["T2"],
                    lot_long=self.executor.lot_long, lot_short=self.executor.lot_short,
                    t1_spot=t1_spot, t2_spot=t2_spot,
                    t1_fill=fills["long_fill"], t2_fill=fills["short_fill"],
                )

        logger.info("── Cycle end ──")

    def _exit(
        self,
        reason: str = "signal",
        z_score: Optional[float] = None,
        t1_spot: Optional[float] = None,
        t2_spot: Optional[float] = None,
    ):
        direction = self.state.direction
        lots      = self.state.lots
        logger.info(f"Exiting {direction} ({lots} lots)  reason={reason}")
        if direction == "long":
            oids = self.executor.exit_long(lots)
        else:
            oids = self.executor.exit_short(lots)
        if not self._legs_ok(oids):
            logger.error("Exit incomplete: one or more legs failed. Position state kept open.")
            return
        # Fetch broker fill prices (best effort) before closing state
        fills = self.executor.get_leg_fills(oids)
        self.trade_log.log_exit(
            reason=reason,
            z_score=z_score or 0.0,
            order_ids=oids,
            t1_spot=t1_spot,
            t2_spot=t2_spot,
            t1_fill=fills["long_fill"],
            t2_fill=fills["short_fill"],
        )
        self.state.close_position()

    @staticmethod
    def _legs_ok(order_ids: dict) -> bool:
        if not isinstance(order_ids, dict):
            return False
        return bool(order_ids.get("long_leg")) and bool(order_ids.get("short_leg"))


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

def is_market_hours(cfg: ConfigParser) -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    open_h,  open_m  = map(int, cfg["TIMING"]["market_open"].split(":"))
    close_h, close_m = map(int, cfg["TIMING"]["market_close"].split(":"))
    open_t  = now.replace(hour=open_h,  minute=open_m,  second=0, microsecond=0)
    close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return open_t <= now <= close_t




def is_execution_window(cfg):
    """
    Checks if current time is within allowed trading hours,
    excluding the first hour of the market.
    """
    now = datetime.now(IST)

    # 1. Weekend Check (NSE is closed Sat/Sun)
    if now.weekday() >= 5:
        return False

    current_time = now.time()

    # 2. Market timings from config.ini
    market_open_h, market_open_m = map(int, cfg["TIMING"]["market_open"].split(":"))
    market_close_h, market_close_m = map(int, cfg["TIMING"]["market_close"].split(":"))
    market_open = dt_time(market_open_h, market_open_m)
    market_close = dt_time(market_close_h, market_close_m)

    # 3. Buffer Check: Skip the first hour (Execution allowed after 10:15)
    # This avoids the "Opening Gap" volatility where spreads are often fake.
    execution_start = dt_time(market_open_h + 1, market_open_m)

    if execution_start <= current_time <= market_close:
        return True

    return False


def _load_env():
    """Load .env and remap custom var names to what upstox-totp expects."""
    load_dotenv()
    remap = {
        "UPSTOX_API_KEY": "UPSTOX_CLIENT_ID",
        "UPSTOX_SECRET":  "UPSTOX_CLIENT_SECRET",
        "TOTP_KEY":       "UPSTOX_TOTP_SECRET",
    }
    for src, dst in remap.items():
        if os.environ.get(src) and not os.environ.get(dst):
            os.environ[dst] = os.environ[src]


def main():
    _load_env()
    cfg = load_config()
    setup_logging(cfg["PATHS"]["log_file"])

    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  NHPC/POWERGRID Stat-Arb Bot — Upstox   ║")
    logger.info("╚══════════════════════════════════════════╝")

    bot = StatArbBot(cfg)
    bot.login()

    def scheduled_cycle():
        # Using the new window check
        if is_execution_window(cfg):
            try:
                logger.info("Execution window open. Running cycle...")
                bot.run_cycle()
            except Exception as e:
                logger.exception(f"Unhandled error in cycle: {e}")
        else:
            # Check if it's specifically the "First Hour" or just "Closed"
            now_time = datetime.now(IST).time()
            market_open_h, market_open_m = map(int, cfg["TIMING"]["market_open"].split(":"))
            execution_start = dt_time(market_open_h + 1, market_open_m)
            if dt_time(market_open_h, market_open_m) <= now_time < execution_start:
                logger.info("In first hour of market — skipping for volatility buffer.")
            else:
                logger.info("Outside market hours or Weekend — skipping.")

    # Schedule login daily at the configured time
    login_time = cfg["TIMING"]["login_time"]
    schedule.every().day.at(login_time).do(bot.login)

    # NEW: Check every 15 minutes instead of every hour
    schedule.every(15).minutes.at(":00").do(scheduled_cycle)

    # Run immediately on startup (if window is open)
    scheduled_cycle()

    open_h, open_m = map(int, cfg["TIMING"]["market_open"].split(":"))
    first_hour_end = dt_time(open_h + 1, open_m).strftime("%H:%M")
    logger.info(
        f"Bot Active. Checking signals every 15 mins. "
        f"(Skip first hour after open: {cfg['TIMING']['market_open']}-{first_hour_end})"
    )
    while True:
        schedule.run_pending()
        time.sleep(1)  # Faster heartbeat for the schedule runner


if __name__ == "__main__":
    main()

