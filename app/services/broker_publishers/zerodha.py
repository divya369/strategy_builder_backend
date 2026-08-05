from __future__ import annotations
import hashlib
import logging
from typing import Any, Dict, List
import pandas as pd
import requests
from .base import BrokerPublisherAdapter
from sqlalchemy import text
from app.core.config import settings
from app.core.broker_token_store import get_broker_token
from app.core.database import EquitycaseSessionLocal

logger = logging.getLogger(__name__)

# Kite LTP API supports up to 1000 instruments per request.
_KITE_LTP_CHUNK_SIZE = 500

# NSE cash-market tick size. Kite rejects LIMIT prices that aren't a multiple of this.
_NSE_TICK = 0.05

# BE-series (Trade-to-Trade) stocks below this market cap cannot take MARKET orders
# on the Publisher Offsite flow (market_protection is unsupported there), so we place
# LIMIT orders for them to avoid rejection. EQ series and larger BE stocks stay MARKET.
_BE_MARKET_ORDER_MIN_MCAP_CRORES = 500.0


def _round_to_tick(price: float, side: str) -> float:
    """Round a price to a valid NSE tick (₹0.05), nudged toward the fill side.

    A BUY limit rounds UP and a SELL limit rounds DOWN, so the limit is slightly
    aggressive and more likely to fill on illiquid BE stocks. Returns 0.0 for a
    non-positive/invalid price.
    """
    import math
    if price is None or price <= 0:
        return 0.0
    ticks = price / _NSE_TICK
    if str(side).upper() == "BUY":
        rounded = math.ceil(ticks - 1e-9) * _NSE_TICK
    else:  # SELL
        rounded = math.floor(ticks + 1e-9) * _NSE_TICK
    return round(rounded, 2)


def _resolve_order_type(is_be: bool, mcap_crores, side: str, base_price) -> Dict[str, Any]:
    """Decide MARKET vs LIMIT for one order and compute the LIMIT price if needed.

    Rule: BE-series AND (market cap unknown OR < ₹500 Cr) → LIMIT; everything else
    (EQ, or BE ≥ 500 Cr) → MARKET. Unknown market cap defaults to LIMIT because we
    cannot confirm the stock is above the threshold, so limit-ordering avoids a reject.

    Returns {"order_type": "MARKET"} or {"order_type": "LIMIT", "price": <tick-rounded>}.
    """
    needs_limit = is_be and (
        mcap_crores is None or float(mcap_crores) < _BE_MARKET_ORDER_MIN_MCAP_CRORES
    )
    if not needs_limit:
        return {"order_type": "MARKET"}
    try:
        price = float(base_price) if base_price is not None and not pd.isna(base_price) else None
    except (TypeError, ValueError):
        price = None
    return {"order_type": "LIMIT", "price": _round_to_tick(price, side) if price else 0.0}


class ZerodhaPublisherAdapter(BrokerPublisherAdapter):
    broker = "ZERODHA"

    # ── Kite LTP fetch ────────────────────────────────────────────────────

    @staticmethod
    def fetch_ltp_bulk(symbols: List[str], exchange: str = "NSE") -> Dict[str, float]:
        """Fetch real-time LTP from Kite Connect for multiple symbols in one request.

        Kite API: GET https://api.kite.trade/quote/ltp?i=NSE:SYM1&i=NSE:SYM2
        Auth:     Authorization: token api_key:access_token
        Limit:    Up to 1000 instruments per request.

        Returns: {tradingsymbol: last_price} dict. Missing symbols are omitted.
        """
        api_key = settings.ZERODHA_API_KEY
        access_token = get_broker_token()

        if not api_key or not access_token:
            logger.warning("[KiteLTP] API key or access token not configured — skipping LTP fetch")
            return {}

        ltp_map: Dict[str, float] = {}

        # Build instrument keys: "NSE:RELIANCE", "NSE:INFY", etc.
        # Include the -BE suffix variant to capture Trade-to-Trade stocks as well.
        instrument_keys = []
        for sym in symbols:
            instrument_keys.append(f"{exchange}:{sym}")
            instrument_keys.append(f"{exchange}:{sym}-BE")

        # Chunk to stay within Kite's limit (max 1000, we use 500 for safety)
        for chunk_start in range(0, len(instrument_keys), _KITE_LTP_CHUNK_SIZE):
            chunk = instrument_keys[chunk_start: chunk_start + _KITE_LTP_CHUNK_SIZE]

            try:
                resp = requests.get(
                    "https://api.kite.trade/quote/ltp",
                    params=[("i", key) for key in chunk],
                    headers={
                        "X-Kite-Version": "3",
                        "Authorization": f"token {api_key}:{access_token}",
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json().get("data", {})

                for inst_key, quote in data.items():
                    # inst_key = "NSE:RELIANCE" or "NSE:RELIANCE-BE" → tradingsymbol = "RELIANCE"
                    tradingsymbol = inst_key.split(":", 1)[-1] if ":" in inst_key else inst_key
                    if tradingsymbol.endswith("-BE"):
                        tradingsymbol = tradingsymbol[:-3]
                    
                    last_price = quote.get("last_price")
                    if last_price is not None:
                        # If a stock exists as both EQ and BE (rare), we take whichever returned a price.
                        # If both returned, they overwrite, but usually only one is active.
                        ltp_map[tradingsymbol] = float(last_price)

            except requests.exceptions.RequestException as e:
                logger.error("[KiteLTP] HTTP error fetching LTP chunk: %s", e)
            except Exception as e:
                logger.error("[KiteLTP] Unexpected error fetching LTP chunk: %s", e)

        logger.info("[KiteLTP] Fetched LTP for %d / %d symbols", len(ltp_map), len(symbols))
        return ltp_map

    # ── Payload builder ───────────────────────────────────────────────────

    def build_payload(self, *, strategy: Any, buy_df: pd.DataFrame, sell_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Zerodha Kite Publisher / Offsite basket payload.
        Uses MARKET orders, except BE-series stocks under ₹500 Cr market cap which use
        tick-aligned LIMIT orders (MARKET is rejected for those on the offsite flow).
        Frontend should submit `basket` to the Zerodha offsite/publisher flow.
        """
        # Collect all unique symbols from buy + sell
        all_symbols = set()
        if not sell_df.empty:
            all_symbols.update(sell_df["tradingsymbol"].dropna().unique())
        if not buy_df.empty:
            all_symbols.update(buy_df["tradingsymbol"].dropna().unique())

        # ── LTP fetch commented out — not needed for MARKET orders ──
        # Kept for future LIMIT order support.
        # ltp_map = self.fetch_ltp_bulk(list(all_symbols)) if all_symbols else {}
        # logger.info("[KiteBasket] LTP map has %d entries for %d symbols. LTP map: %s",
        #              len(ltp_map), len(all_symbols), ltp_map)

        # ── Look up BE series symbols from equitycase DB ──
        # Kite requires '-BE' suffix for Trade-to-Trade (BE series) stocks.
        be_symbols = set()
        if all_symbols:
            ec_db = EquitycaseSessionLocal()
            try:
                rows = ec_db.execute(
                    text("SELECT tradingsymbol FROM equity.equity_metadata WHERE series = 'BE' AND tradingsymbol = ANY(:symbols)"),
                    {"symbols": list(all_symbols)},
                ).fetchall()
                be_symbols = {row[0] for row in rows}
            except Exception as e:
                logger.warning("[KiteBasket] Failed to look up BE series from equity_metadata: %s", e)
            finally:
                ec_db.close()
        if be_symbols:
            logger.info("[KiteBasket] BE series symbols found: %s", be_symbols)

        # ── Look up market cap (₹ Cr) from the daily screener CSV ──
        # Used only to decide MARKET vs LIMIT for BE stocks (see _resolve_order_type).
        # The daily screener CSV is the full universe, so it covers buys AND sells.
        # If it can't be loaded, BE stocks default to LIMIT (safe — avoids rejection).
        mcap_map: Dict[str, float] = {}
        if all_symbols:
            try:
                from app.services import csv_data_service
                from datetime import date as _date
                latest = csv_data_service.get_latest_screener_date() or _date.today()
                sdf = csv_data_service.get_screener_data(latest)
                if sdf is not None and not sdf.empty and \
                        "tradingsymbol" in sdf.columns and "market_cap_crores" in sdf.columns:
                    sub = sdf[["tradingsymbol", "market_cap_crores"]].dropna(subset=["tradingsymbol"])
                    mcap_map = {
                        str(sym): (float(mc) if pd.notna(mc) else None)
                        for sym, mc in zip(sub["tradingsymbol"], sub["market_cap_crores"])
                    }
            except Exception as e:
                logger.warning("[KiteBasket] Could not load market caps from screener CSV — "
                               "BE stocks will default to LIMIT orders: %s", e)

        orders = []

        for _, row in sell_df.iterrows():
            if int(row.get("qty") or 0) <= 0:
                continue
            tag_val = row.get("publisher_tag")
            tag = "" if pd.isna(tag_val) else str(tag_val or "")
            tradingsymbol = row["tradingsymbol"]
            if not tag:
                logger.warning("[KiteBasket] SELL order for %s has EMPTY publisher_tag — postback matching will fail!", tradingsymbol)

            is_be = tradingsymbol in be_symbols
            resolved = _resolve_order_type(is_be, mcap_map.get(tradingsymbol), "SELL", row.get("price"))
            order = {
                "exchange": "NSE",
                "tradingsymbol": f"{tradingsymbol}-BE" if is_be else tradingsymbol,
                "transaction_type": "SELL",
                "quantity": int(row["qty"]),
                "product": "CNC",
                "order_type": resolved["order_type"],
                "validity": "DAY",
                "readonly": False,
                "tag": tag,
            }
            if resolved["order_type"] == "LIMIT":
                order["price"] = resolved["price"]
                logger.info("[KiteBasket] SELL %s → LIMIT @ %.2f (BE, mcap=%s Cr)",
                            tradingsymbol, resolved["price"], mcap_map.get(tradingsymbol))
            orders.append(order)

        for _, row in buy_df.iterrows():
            if int(row.get("qty") or 0) <= 0:
                continue
            tag_val = row.get("publisher_tag")
            tag = "" if pd.isna(tag_val) else str(tag_val or "")
            tradingsymbol = row["tradingsymbol"]
            if not tag:
                logger.warning("[KiteBasket] BUY order for %s has EMPTY publisher_tag — postback matching will fail!", tradingsymbol)

            is_be = tradingsymbol in be_symbols
            resolved = _resolve_order_type(is_be, mcap_map.get(tradingsymbol), "BUY", row.get("price"))
            order = {
                "exchange": "NSE",
                "tradingsymbol": f"{tradingsymbol}-BE" if is_be else tradingsymbol,
                "transaction_type": "BUY",
                "quantity": int(row["qty"]),
                "product": "CNC",
                "order_type": resolved["order_type"],
                "validity": "DAY",
                "readonly": False,
                "tag": tag,
            }
            if resolved["order_type"] == "LIMIT":
                order["price"] = resolved["price"]
                logger.info("[KiteBasket] BUY %s → LIMIT @ %.2f (BE, mcap=%s Cr)",
                            tradingsymbol, resolved["price"], mcap_map.get(tradingsymbol))
            orders.append(order)

        return {
            "broker": self.broker,
            "broker_account_label": strategy.broker_account_label,
            "mode": "ZERODHA_PUBLISHER_OFFSITE",
            "basket": orders,
        }

    def normalize_postback(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Kite Publisher postback into internal standard format.

        Kite postback statuses: COMPLETE, CANCEL, REJECTED, UPDATE.
        Note: Kite sends 'CANCEL' (not 'CANCELLED').

        CRITICAL: Do NOT use `x or fallback` for numeric fields like
        filled_quantity / average_price because 0 is a valid value
        (e.g. REJECTED orders have filled_quantity=0, average_price=0).
        Using `or` would treat 0 as falsy and fall through to the wrong field.
        """
        # filled_quantity: use Kite's filled_quantity directly. Only fall back
        # to quantity if filled_quantity key is completely absent (not just 0).
        filled_quantity = payload.get("filled_quantity")
        if filled_quantity is None:
            filled_quantity = payload.get("quantity") or 0

        # average_price: use Kite's average_price directly. Only fall back
        # to price if average_price key is completely absent (not just 0).
        average_price = payload.get("average_price")
        if average_price is None:
            average_price = payload.get("price") or 0

        # Kite sends 'CANCEL', normalize to 'CANCELLED' for internal consistency.
        status = (payload.get("status") or "").upper()
        if status == "CANCEL":
            status = "CANCELLED"

        return {
            "tag": payload.get("tag"),
            "order_id": payload.get("order_id"),
            "status": status,
            "filled_quantity": int(filled_quantity),
            "average_price": float(average_price),
            "client_id": payload.get("user_id") or payload.get("placed_by"),
            "transaction_type": payload.get("transaction_type"),
            "tradingsymbol": payload.get("tradingsymbol"),
            "status_message": payload.get("status_message"),
            "status_message_raw": payload.get("status_message_raw"),
            "exchange": payload.get("exchange"),
            "raw": payload,
        }

    def verify_checksum(self, payload: Dict[str, Any]) -> bool | None:
        """Verify Kite postback authenticity using checksum.

        Kite generates: checksum = SHA256(order_id + order_timestamp + api_secret)

        Returns:
            True  — checksum is valid (postback is authentic)
            False — checksum is invalid (postback is tampered/fake)
            None  — cannot verify (api_secret not configured or fields missing)
        """
        api_secret = settings.ZERODHA_PUBLISHER_API_SECRET
        if not api_secret:
            return None  # Cannot verify without publisher api_secret

        checksum = payload.get("checksum")
        order_id = payload.get("order_id")
        order_timestamp = payload.get("order_timestamp")

        if not checksum or not order_id or not order_timestamp:
            return None  # Cannot verify without required fields

        expected = hashlib.sha256(
            (str(order_id) + str(order_timestamp) + api_secret).encode("utf-8")
        ).hexdigest()

        return checksum == expected

    # ── Orderbook fetch (read-only via Publisher access_token) ─────────────

    def fetch_orderbook(self, user_access_token: str) -> list:
        """Fetch full day's orderbook for a user via Kite Connect API.

        Uses the PUBLISHER API key (ZERODHA_PUBLISHER_API_KEY) + the user's
        access_token obtained via Publisher token exchange. This gives us
        read-only access to the user's orderbook for the current day.

        GET https://api.kite.trade/orders
        Authorization: token <publisher_api_key>:<user_access_token>

        Returns:
            List of order dicts from Kite, or empty list on failure.
            Each order has: tag, order_id, status, filled_quantity,
            average_price, tradingsymbol, transaction_type, placed_by, etc.
        """
        api_key = settings.ZERODHA_PUBLISHER_API_KEY
        if not api_key or not user_access_token:
            logger.warning("[Orderbook] Missing API key or access_token — cannot fetch orderbook")
            return []

        try:
            resp = requests.get(
                "https://api.kite.trade/orders",
                headers={
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{user_access_token}",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("data", [])
            logger.info("[Orderbook] Fetched %d orders from Kite", len(data))
            return data
        except requests.exceptions.HTTPError as e:
            logger.error("[Orderbook] Kite API HTTP error: %s — %s", e, getattr(e.response, 'text', ''))
            return []
        except Exception as e:
            logger.error("[Orderbook] Failed to fetch orderbook: %s", e)
            return []

    # ── Token exchange (Publisher redirect → access_token) ────────────────

    def exchange_token(self, request_token: str) -> Dict[str, Any]:
        """Exchange Kite Publisher request_token for access_token.

        Uses: SHA256(api_key + request_token + api_secret) as checksum.
        POST to https://api.kite.trade/session/token

        The resulting access_token can be used for read-only operations:
        user orders, holdings, margins, etc.
        """
        from datetime import datetime, timedelta, time, date as date_cls

        api_key = settings.ZERODHA_PUBLISHER_API_KEY
        api_secret = settings.ZERODHA_PUBLISHER_API_SECRET

        if not api_key or not api_secret:
            raise ValueError("ZERODHA_PUBLISHER_API_KEY or ZERODHA_PUBLISHER_API_SECRET not configured")

        checksum = hashlib.sha256(
            f"{api_key}{request_token}{api_secret}".encode()
        ).hexdigest()

        resp = requests.post(
            "https://api.kite.trade/session/token",
            headers={"X-Kite-Version": "3"},
            data={
                "api_key": api_key,
                "request_token": request_token,
                "checksum": checksum,
            },
            timeout=10,
        )

        if resp.status_code != 200:
            # Log the FULL Kite error response before raising
            try:
                error_body = resp.json()
                kite_message = error_body.get("message", "")
                kite_error_type = error_body.get("error_type", "")
            except Exception:
                kite_message = resp.text
                kite_error_type = ""
            logger.error(
                "[TokenExchange] Kite returned HTTP %d | message=%s | error_type=%s | request_token=%s...%s",
                resp.status_code, kite_message, kite_error_type,
                request_token[:8], request_token[-4:] if len(request_token) > 8 else "",
            )
            raise ValueError(
                f"Kite token exchange failed (HTTP {resp.status_code}): {kite_message} [{kite_error_type}]"
            )

        data = resp.json()["data"]

        # Token expires around 6 AM next day
        expires_at = datetime.combine(
            date_cls.today() + timedelta(days=1),
            time(hour=6, minute=0),
        )

        return {
            "access_token": data["access_token"],
            "user_id": data.get("user_id"),
            "user_name": data.get("user_name"),
            "user_shortname": data.get("user_shortname"),
            "email": data.get("email"),
            "broker": data.get("broker"),
            "login_time": data.get("login_time"),
            "expires_at": expires_at,
            "profile": {
                "user_id": data.get("user_id"),
                "user_name": data.get("user_name"),
                "user_shortname": data.get("user_shortname"),
                "email": data.get("email"),
                "broker": data.get("broker"),
                "login_time": data.get("login_time"),
            },
        }

