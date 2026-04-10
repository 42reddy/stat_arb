"""
auth.py  —  Upstox headless TOTP login
────────────────────────────────────────
Uses the `upstox-totp` community package for clean headless auth.
Caches the daily token to disk. Token valid until 3:30 AM next day.

Install: pip install upstox-totp upstox-python-sdk
"""

import json
import logging
import os
import time
from configparser import ConfigParser
from datetime import date

import upstox_client
from upstox_totp import UpstoxTOTP

logger = logging.getLogger(__name__)


def get_client(cfg: ConfigParser) -> tuple[upstox_client.ApiClient, str]:
    """
    Returns (ApiClient, access_token).
    Uses sandbox mode if [SANDBOX] enabled = true in config.

    The ApiClient is pre-configured and ready to pass to any Upstox API class.
    """
    token_file  = cfg["PATHS"]["token_file"]
    sandbox     = cfg["SANDBOX"].getboolean("enabled", fallback=True)
    os.makedirs(os.path.dirname(token_file), exist_ok=True)

    today        = date.today().isoformat()
    access_token = None

    # ── Try cached token ─────────────────────────────────────────
    if os.path.exists(token_file):
        try:
            with open(token_file) as f:
                cache = json.load(f)
            if cache.get("date") == today:
                access_token = cache["access_token"]
                logger.info("Using cached access_token from today")
        except Exception as e:
            logger.warning(f"Token cache read failed: {e}")

    # ── Fresh login if needed ────────────────────────────────────
    if not access_token:
        if sandbox:
            # 1. Pull the manual token from config
            access_token = cfg["UPSTOX"].get("sandbox_token")
            if not access_token:
                raise RuntimeError("SANDBOX ENABLED: 'sandbox_token' missing in config.ini. Please paste your manual token.")

            # 2. Update the cache
            with open(token_file, "w") as f:
                json.dump({"date": today, "access_token": access_token}, f)

            logger.info("Sandbox token applied and cached to disk")

        else:
            # LIVE MODE: Fresh TOTP login
            logger.info("No valid cached token — performing fresh TOTP login for LIVE mode")

            # upstox-totp reads credentials from env vars
            os.environ["UPSTOX_USERNAME"]    = cfg["UPSTOX"]["mobile"]
            os.environ["UPSTOX_PASSWORD"]    = cfg["UPSTOX"]["password"]
            os.environ["UPSTOX_PIN_CODE"]    = cfg["UPSTOX"]["pin"]
            os.environ["UPSTOX_TOTP_SECRET"] = cfg["UPSTOX"]["totp_secret"]
            os.environ["UPSTOX_CLIENT_ID"]   = cfg["UPSTOX"]["client_id"]
            os.environ["UPSTOX_CLIENT_SECRET"]= cfg["UPSTOX"]["client_secret"]
            os.environ["UPSTOX_REDIRECT_URI"]= cfg["UPSTOX"]["redirect_uri"]

            for attempt in range(1, 4):
                try:
                    upx      = UpstoxTOTP()
                    response = upx.app_token.get_access_token()
                    if not response.success or not response.data:
                        raise RuntimeError(f"upstox-totp returned failure: {response}")

                    access_token = response.data.access_token
                    logger.info(f"Logged in as {response.data.user_name} ({response.data.user_id})")
                    break
                except Exception as e:
                    logger.error(f"Login attempt {attempt} failed: {e}")
                    if attempt < 3:
                        time.sleep(10)
            else:
                raise RuntimeError("All login attempts failed — check credentials in config.ini")

            # Cache the live token
            with open(token_file, "w") as f:
                json.dump({"date": today, "access_token": access_token}, f)
            logger.info("access_token cached to disk")

    # ── Build ApiClient ──────────────────────────────────────────
    configuration = upstox_client.Configuration(sandbox=sandbox)
    configuration.access_token = access_token

    if sandbox:
        logger.info("★ SANDBOX MODE — no real orders will be placed ★")
    else:
        logger.info("★ LIVE MODE — real orders will be placed ★")

    api_client = upstox_client.ApiClient(configuration)
    return api_client, access_token
