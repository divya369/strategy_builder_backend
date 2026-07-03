from __future__ import annotations
import hashlib
import logging
from typing import Any, Dict, List
import pandas as pd
import requests
from .base import BrokerPublisherAdapter
from app.core.config import settings
from app.core.broker_token_store import get_broker_token

logger = logging.getLogger(__name__)

# Kite LTP API supports up to 1000 instruments per request.
_KITE_LTP_CHUNK_SIZE = 500


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
        Uses LIMIT orders with real-time LTP from Kite as the limit price.
        Frontend should submit `basket` to Zerodha offsite/publisher flow.
        """
        # Collect all unique symbols from buy + sell to fetch LTP in one batch
        all_symbols = set()
        if not sell_df.empty:
            all_symbols.update(sell_df["tradingsymbol"].dropna().unique())
        if not buy_df.empty:
            all_symbols.update(buy_df["tradingsymbol"].dropna().unique())

        # Fetch live LTP from Kite API (single batch request)
        ltp_map = self.fetch_ltp_bulk(list(all_symbols)) if all_symbols else {}
        logger.info("[KiteBasket] LTP map has %d entries for %d symbols. LTP map: %s",
                     len(ltp_map), len(all_symbols), ltp_map)

        orders = []

        for _, row in sell_df.iterrows():
            if int(row.get("qty") or 0) <= 0:
                continue
            tag_val = row.get("publisher_tag")
            tag = "" if pd.isna(tag_val) else str(tag_val or "")
            tradingsymbol = row["tradingsymbol"]

            # Use live LTP if available, otherwise fall back to existing price from DB
            limit_price = ltp_map.get(tradingsymbol)
            if limit_price is None or limit_price <= 0:
                # Fallback: use price from buy/sell table (screener close or tradelog LTP)
                fallback = row["price"]
                limit_price = float(fallback) if fallback is not None and not pd.isna(fallback) else 0.0
                logger.warning("[KiteBasket] No LTP for %s, using fallback price=%.2f", tradingsymbol, limit_price)

            orders.append({
                "exchange": "NSE",
                "tradingsymbol": tradingsymbol,
                "transaction_type": "SELL",
                "quantity": int(row["qty"]),
                "product": "CNC",
                "order_type": "MARKET",
                "validity": "DAY",
                "readonly": False,
                "tag": tag,
            })

            # orders.append({
            #     "exchange": "NSE",
            #     "tradingsymbol": tradingsymbol,
            #     "transaction_type": "SELL",
            #     "quantity": int(row["qty"]),
            #     "product": "CNC",
            #     "order_type": "LIMIT",
            #     "price": round(limit_price, 2),
            #     "validity": "DAY",
            #     "readonly": False,
            #     "tag": tag,
            # })

        for _, row in buy_df.iterrows():
            if int(row.get("qty") or 0) <= 0:
                continue
            tag_val = row.get("publisher_tag")
            tag = "" if pd.isna(tag_val) else str(tag_val or "")
            tradingsymbol = row["tradingsymbol"]

            # Use live LTP if available, otherwise fall back to existing price from DB
            limit_price = ltp_map.get(tradingsymbol)
            if limit_price is None or limit_price <= 0:
                # Fallback: use price from buy/sell table (screener close)
                fallback = row["price"]
                limit_price = float(fallback) if fallback is not None and not pd.isna(fallback) else 0.0
                logger.warning("[KiteBasket] No LTP for %s, using fallback price=%.2f", tradingsymbol, limit_price)

            orders.append({
                "exchange": "NSE",
                "tradingsymbol": tradingsymbol,
                "transaction_type": "BUY",
                "quantity": int(row["qty"]),
                "product": "CNC",
                "order_type": "MARKET",
                "validity": "DAY",
                "readonly": False,
                "tag": tag,
            })

            # orders.append({
            #     "exchange": "NSE",
            #     "tradingsymbol": tradingsymbol,
            #     "transaction_type": "BUY",
            #     "quantity": int(row["qty"]),
            #     "product": "CNC",
            #     "order_type": "LIMIT",
            #     "price": round(limit_price, 2),
            #     "validity": "DAY",
            #     "readonly": False,
            #     "tag": tag,
            # })

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

