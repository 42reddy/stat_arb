"""
data.py  —  Market data via yfinance (spot prices for signal generation)
────────────────────────────────────────────────────────────────────────
Downloads 15-minute OHLCV for both legs, aligns on common timestamps,
and returns a clean DataFrame ready for features.py.

yfinance limitation: 15-minute data is only available for a shorter window.
For backtest / training use daily/hourly data instead.
"""

import logging
from configparser import ConfigParser

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# How many calendar days of 15-minute data to fetch.
# yfinance supports up to ~60 days for 15m; keep a safety buffer for indicators.
LOOKBACK_DAYS = 60


def fetch_spot(cfg: ConfigParser) -> pd.DataFrame:
    """
    Downloads 15-minute spot prices for both tickers.
    Returns DataFrame with columns [ticker_long, ticker_short]
    indexed by datetime (IST-aware).

    Raises ValueError if too few bars are returned.
    """
    t1 = cfg["STRATEGY"]["ticker_long"]
    t2 = cfg["STRATEGY"]["ticker_short"]

    logger.info(f"Fetching {LOOKBACK_DAYS}d 15m data: {t1}, {t2}")

    raw = yf.download(
        tickers=[t1, t2],
        period=f"{LOOKBACK_DAYS}d",
        interval="15m",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )

    # Extract Close prices for each ticker
    try:
        p1 = raw[t1]["Close"].rename(t1)
        p2 = raw[t2]["Close"].rename(t2)
    except KeyError as e:
        raise ValueError(f"yfinance did not return expected columns: {e}")

    df = pd.concat([p1, p2], axis=1).dropna()

    # Keep only market hours (9:15–15:30 IST)
    # yfinance returns UTC; NSE is UTC+5:30
    if df.index.tz is not None:
        df.index = df.index.tz_convert("Asia/Kolkata")
    else:
        df.index = df.index.tz_localize("UTC").tz_convert("Asia/Kolkata")

    df = df.between_time("09:15", "15:30")
    df = df[df.index.weekday < 5]          # drop weekends (shouldn't appear, but safe)
    df = df.sort_index()

    if len(df) < 50:
        raise ValueError(
            f"Too few 15m bars after filtering: {len(df)}. "
            f"Check ticker names and yfinance availability."
        )

    logger.info(f"Data fetched: {len(df)} 15m bars  [{df.index[0]} → {df.index[-1]}]")
    return df


def latest_bar(df: pd.DataFrame) -> pd.Series:
    """Returns the most recent row."""
    return df.iloc[-1]
