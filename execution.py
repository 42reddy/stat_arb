"""
execution.py  —  Order placement via Upstox API v3 (NFO futures)
─────────────────────────────────────────────────────────────────
Product code "D" = NRML (delivery/carry overnight for futures).
Uses MARKET orders. Sandbox mode is controlled in config.ini —
no separate DRY_RUN flag needed since Upstox has a real sandbox.

Instrument token format: NSE_FO|<token>
Look up tokens at:
  https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
  Filter by: instrument_type = "FUT", name = "NHPC" or "POWERGRID"
  Use the current month's expiry contract.
"""

import logging
from configparser import ConfigParser
from typing import Optional

import upstox_client
from upstox_client.rest import ApiException

logger = logging.getLogger(__name__)


class Executor:
    def __init__(self, api_client: upstox_client.ApiClient, cfg: ConfigParser):
        self.order_api   = upstox_client.OrderApiV3(api_client)
        self.portfolio_api = upstox_client.PortfolioApi(api_client)
        self.cfg         = cfg
        self.tok_long    = cfg["STRATEGY"]["fut_long_token"]
        self.tok_short   = cfg["STRATEGY"]["fut_short_token"]
        self.lot_long    = int(cfg["STRATEGY"]["lot_long"])
        self.lot_short   = int(cfg["STRATEGY"]["lot_short"])
        self.name_long   = cfg["STRATEGY"].get("fut_long_name",  "FUT_LONG")
        self.name_short  = cfg["STRATEGY"].get("fut_short_name", "FUT_SHORT")

    # ── Internal ──────────────────────────────────────────────────

    def _place(
        self,
        instrument_token: str,
        transaction_type: str,   # "BUY" or "SELL"
        quantity: int,
        tag: str = "statarb",
    ) -> Optional[str]:
        """Place a single NRML market order. Returns order_id or None."""
        body = upstox_client.PlaceOrderV3Request(
            quantity          = quantity,
            product           = "D",          # D = NRML (carry overnight)
            validity          = "DAY",
            price             = 0,            # 0 = market order
            instrument_token  = instrument_token,
            order_type        = "MARKET",
            transaction_type  = transaction_type,
            disclosed_quantity= 0,
            trigger_price     = 0,
            is_amo            = False,
            tag               = tag,
        )
        try:
            resp = self.order_api.place_order(body, algo_name="statarb")
            order_id = str(resp.data.order_id)
            logger.info(
                f"Order placed: {transaction_type} {quantity}x {instrument_token}  "
                f"order_id={order_id}"
            )
            return order_id
        except ApiException as e:
            logger.error(
                f"Order FAILED: {transaction_type} {quantity}x {instrument_token}  "
                f"status={e.status}  body={e.body}"
            )
            return None

    # ── Public interface ──────────────────────────────────────────

    def enter_long(self, lots: int = 1) -> dict:
        """
        Long spread: BUY fut_long + SELL fut_short
        (spread too low, expect mean reversion upward)
        """
        logger.info(f"ENTERING LONG spread ({lots} lot(s))")
        oid_l = self._place(self.tok_long,  "BUY",  lots * self.lot_long)
        oid_s = self._place(self.tok_short, "SELL", lots * self.lot_short)
        return {"long_leg": oid_l, "short_leg": oid_s}

    def enter_short(self, lots: int = 1) -> dict:
        """
        Short spread: SELL fut_long + BUY fut_short
        (spread too high, expect mean reversion downward)
        """
        logger.info(f"ENTERING SHORT spread ({lots} lot(s))")
        oid_l = self._place(self.tok_long,  "SELL", lots * self.lot_long)
        oid_s = self._place(self.tok_short, "BUY",  lots * self.lot_short)
        return {"long_leg": oid_l, "short_leg": oid_s}

    def exit_long(self, lots: int) -> dict:
        """Unwind a long spread position."""
        logger.info(f"EXITING LONG spread ({lots} lot(s))")
        oid_l = self._place(self.tok_long,  "SELL", lots * self.lot_long)
        oid_s = self._place(self.tok_short, "BUY",  lots * self.lot_short)
        return {"long_leg": oid_l, "short_leg": oid_s}

    def exit_short(self, lots: int) -> dict:
        """Unwind a short spread position."""
        logger.info(f"EXITING SHORT spread ({lots} lot(s))")
        oid_l = self._place(self.tok_long,  "BUY",  lots * self.lot_long)
        oid_s = self._place(self.tok_short, "SELL", lots * self.lot_short)
        return {"long_leg": oid_l, "short_leg": oid_s}

    def add_long(self, extra_lots: int) -> dict:
        logger.info(f"PYRAMIDING LONG (+{extra_lots} lot(s))")
        return self.enter_long(extra_lots)

    def add_short(self, extra_lots: int) -> dict:
        logger.info(f"PYRAMIDING SHORT (+{extra_lots} lot(s))")
        return self.enter_short(extra_lots)

    def get_positions(self) -> dict:
        """Fetch current net positions. Returns {instrument_token: position_obj}."""
        try:
            resp = self.portfolio_api.get_positions()
            return {p.instrument_token: p for p in (resp.data or [])}
        except ApiException as e:
            logger.error(f"Failed to fetch positions: {e.body}")
            return {}

    def verify_flat(self) -> bool:
        """Returns True if both futures legs show zero net quantity."""
        positions = self.get_positions()
        qty_long  = getattr(positions.get(self.tok_long),  "quantity", 0) or 0
        qty_short = getattr(positions.get(self.tok_short), "quantity", 0) or 0
        if qty_long != 0 or qty_short != 0:
            logger.warning(
                f"Position mismatch! Upstox shows "
                f"{self.name_long}={qty_long}, {self.name_short}={qty_short}"
            )
            return False
        return True
