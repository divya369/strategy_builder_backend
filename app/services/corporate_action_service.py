"""
Corporate Action Service — Apply corporate actions to live tradelog.

Handles three types of corporate actions for active tradelog holdings:
1. Symbol Rename  — detected from symbol_rename_map.json
2. ISIN Sync      — synced from equity.equity_metadata (equitycase DB)
3. Bonus / Split  — parsed from daily NSE corporate actions CSV

Runs as the FIRST step inside _process_strategy_daily_update(), before
tradelog fill processing and LTP refresh. Only modifies active tradelog rows
in tradelog_automate_equity — no buy_stock/sell_stock/circuit_stock changes.

NSE Corporate Actions CSV columns (downloaded daily):
  SYMBOL, COMPANY NAME, SERIES, PURPOSE, FACE VALUE, EX-DATE,
  RECORD DATE, BOOK CLOSURE START DATE, BOOK CLOSURE END DATE
"""
import io
import re
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import EquitycaseSessionLocal
from app.core.symbol_registry import resolve_stock_symbol

logger = logging.getLogger(__name__)

# ── NSE API constants ─────────────────────────────────────────────────────────

_NSE_BASE_URL = "https://www.nseindia.com/api"
_NSE_PAGE_URL = "https://www.nseindia.com/get-quotes/equity?symbol=LT"

_NSE_HEADERS = {
    "Host": "www.nseindia.com",
    "Referer": "https://www.nseindia.com/get-quotes/equity?symbol=SBIN",
    "X-Requested-With": "XMLHttpRequest",
    "pragma": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
}

# Route for "all equities" corporate actions (CSV response, no date filter)
_NSE_CORP_ACTION_ROUTE = "/corporates-corporateActions?index=equities&csv=true"


def _download_corporate_actions_from_nse(today: date) -> Optional[Path]:
    """Download today's corporate actions CSV from NSE API.

    Mimics the browser session approach (same as the equitycase_logic laptop
    code): establish a session with NSE-specific headers, visit a page to get
    cookies, then hit the corporate actions API endpoint.

    Saves CSV to: {CORPORATE_ACTIONS_CSV_DIR}/all/{today}_corporate_actions.csv
    Returns the file path on success, None on failure.
    """
    csv_dir = settings.CORPORATE_ACTIONS_CSV_DIR
    if not csv_dir:
        return None

    output_dir = Path(csv_dir) / "all"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{today}_corporate_actions.csv"

    # Skip download if file already exists
    if output_path.exists():
        return output_path

    try:
        # Create session with NSE headers and get cookies
        session = requests.Session()
        session.headers.update(_NSE_HEADERS)
        session.get(_NSE_PAGE_URL, timeout=10)

        # Hit NSE corporate actions API (returns all upcoming, we filter by EX-DATE later)
        url = _NSE_BASE_URL + _NSE_CORP_ACTION_ROUTE

        response = session.get(url, timeout=10)
        response.raise_for_status()

        # Handle brotli compression if present
        content_encoding = response.headers.get("Content-Encoding", "")
        if content_encoding == "br":
            try:
                import brotli
                raw = brotli.decompress(response.content)
            except ImportError:
                logger.warning("[CorpAction] brotli not installed, trying raw content")
                raw = response.content
        else:
            raw = response.content

        text_content = raw.decode("utf-8-sig", errors="replace")

        # Try parsing as CSV first (direct CSV response from NSE)
        try:
            df = pd.read_csv(io.StringIO(text_content))
            if not df.empty:
                # Parse EX-DATE for consistent format
                if "EX-DATE" in df.columns:
                    df["EX-DATE"] = pd.to_datetime(df["EX-DATE"], format="mixed", dayfirst=True)
                df.to_csv(output_path, index=False)
                logger.info("[CorpAction] Downloaded corporate actions CSV from NSE: %s (%d rows)", output_path.name, len(df))
                return output_path
            else:
                logger.info("[CorpAction] No corporate actions from NSE for %s", today)
                return None
        except Exception:
            pass

        # Fallback: try parsing as JSON (some NSE endpoints return JSON)
        try:
            import json
            data = json.loads(text_content)
            if isinstance(data, list) and data:
                # Map NSE JSON field names to CSV column names
                column_map = {
                    "symbol": "SYMBOL",
                    "company": "COMPANY NAME",
                    "series": "SERIES",
                    "purpose": "PURPOSE",
                    "faceVal": "FACE VALUE",
                    "exDate": "EX-DATE",
                    "recDate": "RECORD DATE",
                    "bcStartDate": "BOOK CLOSURE START DATE",
                    "bcEndDate": "BOOK CLOSURE END DATE",
                }
                df = pd.DataFrame(data)
                df = df.rename(columns=column_map)
                # Keep only the expected columns (some may not exist)
                expected_cols = list(column_map.values())
                existing_cols = [c for c in expected_cols if c in df.columns]
                df = df[existing_cols]
                if "EX-DATE" in df.columns:
                    df["EX-DATE"] = pd.to_datetime(df["EX-DATE"], format="mixed", dayfirst=True)
                df.to_csv(output_path, index=False)
                logger.info("[CorpAction] Downloaded corporate actions (JSON→CSV) from NSE: %s (%d rows)", output_path.name, len(df))
                return output_path
            else:
                logger.info("[CorpAction] No corporate actions from NSE for %s", today)
                return None
        except (json.JSONDecodeError, ValueError):
            logger.warning("[CorpAction] Could not parse NSE response as CSV or JSON for %s", today)
            return None

    except requests.exceptions.RequestException as exc:
        logger.error("[CorpAction] Failed to download corporate actions from NSE: %s", exc)
        return None
    except Exception as exc:
        logger.error("[CorpAction] Unexpected error downloading corporate actions: %s", exc)
        return None


# ── Cached corporate actions CSV for the day ──────────────────────────────────
# Multiple strategies are processed in the same daily run. We cache the parsed
# CSV so we don't re-read/parse it for every strategy.
_cached_ca_date: Optional[date] = None
_cached_ca_df: Optional[pd.DataFrame] = None


def _load_corporate_actions_csv(today: date) -> Optional[pd.DataFrame]:
    """Load and cache today's NSE corporate actions CSV.

    If the CSV doesn't exist locally, downloads it from NSE first.
    Returns None if the file doesn't exist/download fails or is empty.
    Caches the result so subsequent calls for the same date are free.
    """
    global _cached_ca_date, _cached_ca_df

    if _cached_ca_date == today and _cached_ca_df is not None:
        return _cached_ca_df

    csv_dir = settings.CORPORATE_ACTIONS_CSV_DIR
    if not csv_dir:
        return None

    csv_path = Path(csv_dir) / "all" / f"{today}_corporate_actions.csv"

    # If file doesn't exist, try downloading from NSE
    if not csv_path.exists():
        logger.info("[CorpAction] CSV not found locally for %s, downloading from NSE...", today)
        downloaded_path = _download_corporate_actions_from_nse(today)
        if downloaded_path is None:
            logger.debug("[CorpAction] No corporate actions available for %s", today)
            return None
        csv_path = downloaded_path

    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            logger.debug("[CorpAction] Corporate actions CSV is empty for %s", today)
            return None

        # Normalise column names (strip whitespace, handle case variations)
        df.columns = [c.strip().upper().replace("-", "_") for c in df.columns]

        # Parse EX_DATE to date objects
        if "EX_DATE" in df.columns:
            df["EX_DATE"] = pd.to_datetime(df["EX_DATE"], format="mixed", dayfirst=True).dt.date

        _cached_ca_date = today
        _cached_ca_df = df
        logger.info("[CorpAction] Loaded corporate actions CSV: %s (%d rows)", csv_path.name, len(df))
        return df
    except Exception as exc:
        logger.error("[CorpAction] Failed to read corporate actions CSV %s: %s", csv_path, exc)
        return None


# ── Purpose text parsers ──────────────────────────────────────────────────────

def _parse_bonus_factor(purpose: str) -> Optional[float]:
    """Parse bonus ratio from PURPOSE text.

    Examples:
      "Bonus 1:3"     → (1+3)/3 = 1.333
      "Bonus 2:1"     → (2+1)/1 = 3.0
      "Bonus 1:2"     → (1+2)/2 = 1.5

    Returns adjustment factor or None if parsing fails.
    """
    if not purpose:
        return None
    match = re.search(r'[Bb]onus\s+(\d+)\s*:\s*(\d+)', str(purpose))
    if not match:
        return None
    numerator = int(match.group(1))
    denominator = int(match.group(2))
    if denominator == 0:
        return None
    return (numerator + denominator) / denominator


def _parse_split_factor(purpose: str) -> Optional[float]:
    """Parse face value split ratio from PURPOSE text.

    Examples:
      "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share" → 10/1 = 10
      "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share" → 10/2 = 5
      "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share" → 10/5 = 2

    Returns adjustment factor or None if parsing fails.
    """
    if not purpose:
        return None

    # Pattern: "From Rs X/- ... To Re/Rs Y/-"
    match = re.search(
        r'[Ff]rom\s+(?:Rs|Re)\.?\s*(\d+(?:\.\d+)?)\s*/?\s*-?\s*.*?'
        r'[Tt]o\s+(?:Rs|Re)\.?\s*(\d+(?:\.\d+)?)',
        str(purpose)
    )
    if not match:
        return None

    old_fv = float(match.group(1))
    new_fv = float(match.group(2))
    if new_fv == 0:
        return None
    return old_fv / new_fv


def _classify_purpose(purpose: str) -> str:
    """Classify NSE PURPOSE text into action type.

    Returns: 'bonus', 'split', or 'ignore'
    """
    if not purpose:
        return "ignore"
    p = str(purpose).lower()
    if "bonus" in p and ":" in p:
        return "bonus"
    if "split" in p or "sub-division" in p or "sub division" in p:
        return "split"
    # Dividends, rights issues, AGM, etc. — all ignored
    return "ignore"


# ── Core logic ────────────────────────────────────────────────────────────────

def _apply_symbol_renames(tradelog_df: pd.DataFrame, strategy_id) -> pd.DataFrame:
    """Check symbol_rename_map.json and update tradingsymbol for active holdings.

    Uses the existing resolve_stock_symbol() from symbol_registry which
    auto-reloads the JSON map when the file changes.
    """
    if tradelog_df.empty:
        return tradelog_df

    active_mask = tradelog_df["active"] == True
    if not active_mask.any():
        return tradelog_df

    for idx in tradelog_df.loc[active_mask].index:
        old_symbol = tradelog_df.at[idx, "tradingsymbol"]
        new_symbol = resolve_stock_symbol(old_symbol)
        if new_symbol != old_symbol:
            tradelog_df.at[idx, "tradingsymbol"] = new_symbol
            logger.info(
                "[CorpAction] Symbol renamed in tradelog: %s → %s (strategy %s)",
                old_symbol, new_symbol, strategy_id,
            )

    return tradelog_df


def _sync_isin_from_metadata(tradelog_df: pd.DataFrame, strategy_id) -> pd.DataFrame:
    """Query equity.equity_metadata and update ISIN for active holdings.

    Single batch query for all active symbols. Only updates if ISIN differs.
    """
    if tradelog_df.empty:
        return tradelog_df

    active_mask = tradelog_df["active"] == True
    if not active_mask.any():
        return tradelog_df

    active_symbols = list(tradelog_df.loc[active_mask, "tradingsymbol"].unique())
    if not active_symbols:
        return tradelog_df

    # Query equitycase DB for current ISIN mapping
    ec_db = EquitycaseSessionLocal()
    try:
        rows = ec_db.execute(
            text("SELECT tradingsymbol, isin FROM equity.equity_metadata WHERE tradingsymbol = ANY(:symbols)"),
            {"symbols": active_symbols},
        ).fetchall()
        isin_map = {row[0]: row[1] for row in rows if row[1]}
    except Exception as exc:
        logger.warning("[CorpAction] Failed to query equity_metadata for ISIN sync: %s", exc)
        return tradelog_df
    finally:
        ec_db.close()

    if not isin_map:
        return tradelog_df

    # Update tradelog ISIN where it differs
    for idx in tradelog_df.loc[active_mask].index:
        symbol = tradelog_df.at[idx, "tradingsymbol"]
        current_isin = str(tradelog_df.at[idx, "isin"] or "")
        metadata_isin = isin_map.get(symbol, "")

        if metadata_isin and metadata_isin != current_isin:
            tradelog_df.at[idx, "isin"] = metadata_isin
            logger.info(
                "[CorpAction] ISIN updated: %s → %s for %s (strategy %s)",
                current_isin, metadata_isin, symbol, strategy_id,
            )

    return tradelog_df


def _apply_nse_corporate_actions(
    tradelog_df: pd.DataFrame,
    strategy_id,
    today: date,
) -> pd.DataFrame:
    """Parse today's NSE corporate actions CSV and apply Bonus/Split to active holdings.

    Only processes rows where EX-DATE == today. Dividends and other actions are ignored.
    """
    if tradelog_df.empty:
        return tradelog_df

    active_mask = tradelog_df["active"] == True
    if not active_mask.any():
        return tradelog_df

    ca_df = _load_corporate_actions_csv(today)
    if ca_df is None:
        return tradelog_df

    # Filter to today's EX-DATE only
    if "EX_DATE" not in ca_df.columns:
        logger.warning("[CorpAction] CSV missing EX_DATE column")
        return tradelog_df

    today_actions = ca_df[ca_df["EX_DATE"] == today].copy()
    if today_actions.empty:
        logger.debug("[CorpAction] No corporate actions with EX-DATE=%s", today)
        return tradelog_df

    # Build set of active symbols for quick lookup
    active_symbols = set(tradelog_df.loc[active_mask, "tradingsymbol"])

    for _, ca_row in today_actions.iterrows():
        try:
            symbol = str(ca_row.get("SYMBOL", "")).strip()
            purpose = str(ca_row.get("PURPOSE", "")).strip()

            if not symbol or symbol not in active_symbols:
                continue

            action_type = _classify_purpose(purpose)
            if action_type == "ignore":
                continue

            # Calculate adjustment factor
            if action_type == "bonus":
                factor = _parse_bonus_factor(purpose)
            elif action_type == "split":
                factor = _parse_split_factor(purpose)
            else:
                continue

            if factor is None or factor <= 0 or factor == 1.0:
                logger.warning(
                    "[CorpAction] Could not parse factor for %s: PURPOSE='%s'",
                    symbol, purpose,
                )
                continue

            # Find the active tradelog row(s) for this symbol
            match_mask = active_mask & (tradelog_df["tradingsymbol"] == symbol)
            match_indices = tradelog_df.loc[match_mask].index

            for idx in match_indices:
                old_qty = int(tradelog_df.at[idx, "buy_qty"] or 0)
                old_price = float(tradelog_df.at[idx, "buy_price"] or 0)

                new_qty = int(old_qty * factor)
                new_price = round(old_price / factor, 2)

                tradelog_df.at[idx, "buy_qty"] = new_qty
                tradelog_df.at[idx, "buy_price"] = new_price
                # buy_amount stays the same (qty * price cancels out)

                logger.info(
                    "[CorpAction] Applied %s to %s: qty %d→%d, price %.2f→%.2f "
                    "(factor=%.4f, strategy %s)",
                    action_type.upper(), symbol,
                    old_qty, new_qty, old_price, new_price,
                    factor, strategy_id,
                )

        except Exception as exc:
            logger.error(
                "[CorpAction] Error processing corporate action row for %s: %s",
                ca_row.get("SYMBOL", "?"), exc,
            )
            continue

    return tradelog_df


# ── Public entry point ────────────────────────────────────────────────────────

def apply_corporate_actions_to_strategy(
    db: Session,
    strategy,
    tradelog_df: pd.DataFrame,
    today: date,
) -> pd.DataFrame:
    """Apply all corporate actions to a strategy's active tradelog holdings.

    Called from _process_strategy_daily_update() BEFORE update_tradelog().

    Order of operations:
    1. Symbol Rename  (from symbol_rename_map.json)
    2. ISIN Sync      (from equity.equity_metadata)
    3. Bonus / Split  (from NSE corporate actions CSV, only today's EX-DATE)

    Returns the (possibly modified) tradelog_df.
    """
    if tradelog_df.empty:
        return tradelog_df

    active_count = (tradelog_df["active"] == True).sum()
    if active_count == 0:
        return tradelog_df

    try:
        # 1. Symbol Rename
        tradelog_df = _apply_symbol_renames(tradelog_df, strategy.id)

        # 2. ISIN Sync
        tradelog_df = _sync_isin_from_metadata(tradelog_df, strategy.id)

        # 3. Bonus / Split from NSE CSV
        tradelog_df = _apply_nse_corporate_actions(tradelog_df, strategy.id, today)

    except Exception as exc:
        logger.error(
            "[CorpAction] Unexpected error applying corporate actions for strategy %s: %s",
            strategy.id, exc,
        )

    return tradelog_df
