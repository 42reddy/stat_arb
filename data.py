"""
data.py  —  Market data via yfinance (daily spot prices for signal generation)
──────────────────────────────────────────────────────────────────────────────
Downloads daily OHLCV for both legs, aligns on common timestamps, and returns
a clean DataFrame ready for features.py.

The bot still runs its check loop every 15 minutes, but it fetches daily bars
each time — so signals are re-evaluated against the latest daily close (which
yfinance populates as a partial/current-day bar during market hours).
The model was trained on daily frequency; using anything finer is incorrect.
"""

import logging
from configparser import ConfigParser

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# How many calendar days of daily data to fetch.
# Need at least 4 × slow_window (≈92) trading days for OLS + indicator warmup.
# 1 year (~252 trading days) gives a comfortable buffer.
LOOKBACK_DAYS = 365


def fetch_spot(cfg: ConfigParser) -> pd.DataFrame:
    """
    Downloads daily spot prices for both tickers.
    Returns DataFrame with columns [ticker_long, ticker_short]
    indexed by date (timezone-aware, IST).

    Raises ValueError if too few bars are returned.
    """
    t1 = cfg["STRATEGY"]["ticker_long"]
    t2 = cfg["STRATEGY"]["ticker_short"]

    logger.info(f"Fetching {LOOKBACK_DAYS}d daily data: {t1}, {t2}")

    raw = yf.download(
        tickers=[t1, t2],
        period=f"{LOOKBACK_DAYS}d",
        interval="1d",
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

    # Normalise index timezone to IST
    if df.index.tz is not None:
        df.index = df.index.tz_convert("Asia/Kolkata")
    else:
        df.index = df.index.tz_localize("UTC").tz_convert("Asia/Kolkata")

    df = df.sort_index()

    if len(df) < 100:
        raise ValueError(
            f"Too few daily bars after filtering: {len(df)}. "
            f"Check ticker names and yfinance availability."
        )

    logger.info(f"Data fetched: {len(df)} daily bars  [{df.index[0].date()} → {df.index[-1].date()}]")
    return df


def latest_bar(df: pd.DataFrame) -> pd.Series:
    """Returns the most recent row."""
    return df.iloc[-1]
