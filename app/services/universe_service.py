"""
Universe Service — single source of truth for universe identity, size and validation.

WHY THIS EXISTS:
  "Universe size" (NIFTY 50 → 50, NIFTY 100 → 100, All Stocks → None) is needed
  by several APIs and validated on several write paths. Keeping the ALL → None
  rule and the size lookup in one module stops those copies from drifting apart.

WHERE SIZE COMES FROM:
  app/core/universe_registry.py — a declared map, not a count of CSV rows. An
  incomplete constituent file must never shrink a universe's declared size and
  start rejecting valid screeners. Add new indices to that map.

SIZE IS ALWAYS RESOLVED SERVER-SIDE:
  A size sent by the client is treated as a claim to verify, never as input.
  Trusting it would let a request pair "NIFTY 50" with size 500 and walk straight
  past the max_positions check below.

USAGE:
  Read paths  → attach_size() / describe() / list_universes() to enrich a
                universe before returning it.
  Write paths → validate_universe() to reject impossible configurations.
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from app.core import universe_registry
from app.services import csv_data_service

logger = logging.getLogger(__name__)

ALL = "ALL"
INDEX = "INDEX"

# Sentinel: distinguishes "caller passed no claimed size" from "caller passed None"
# (None is itself a meaningful claim — it means "All Stocks").
_UNSET = object()


# ── Identity ─────────────────────────────────────────────────────────────────

def as_dict(universe: Any) -> Dict[str, Any]:
    """Accept a dict or a Pydantic UniverseConfig and return a plain dict."""
    if universe is None:
        return {}
    if hasattr(universe, "model_dump"):
        return universe.model_dump()
    return dict(universe)


def universe_type(universe: Any) -> str:
    """Normalized universe type — 'ALL' or 'INDEX'. Missing type defaults to ALL."""
    return (as_dict(universe).get("type") or ALL).upper()


def is_all(universe: Any) -> bool:
    """True when the universe is the full stock list (no index restriction)."""
    return universe_type(universe) != INDEX


def index_label(index_name: str) -> str:
    """Display label for an index — matches what the /universes endpoint returns."""
    return str(index_name).replace("_", " ")


# ── Size ─────────────────────────────────────────────────────────────────────

def get_universe_size(universe: Any) -> Optional[int]:
    """
    Number of stocks in this universe, from the central registry.

    None for "All Stocks" (unbounded — the screener CSV decides) and for any
    index with no registry entry, which callers treat as "size unknown".
    """
    if is_all(universe):
        return None
    return universe_registry.get_index_size(as_dict(universe).get("value"))


def attach_size(universe: Any) -> Dict[str, Any]:
    """
    Return a copy of `universe` with a "size" key added (read paths).

    Enriching on read means stored screener versions that predate this field
    still come back with a size — no migration and no backfill needed.
    """
    enriched = as_dict(universe)
    enriched["size"] = get_universe_size(enriched)
    return enriched


def describe(universe: Any) -> Dict[str, Any]:
    """Full descriptor for one universe: {type, value, label, size}."""
    uni = as_dict(universe)
    if is_all(uni):
        return {"type": ALL, "value": ALL, "label": "All Stocks", "size": None}
    value = uni.get("value")
    return {
        "type": "index",
        "value": value,
        "label": index_label(value or ""),
        "size": get_universe_size(uni),
    }


def list_universes() -> List[Dict[str, Any]]:
    """
    Every selectable universe as {type, value, label, size} — All Stocks first,
    then one entry per index CSV.

    The list still comes from the CSV folder (drop a file in, it appears), while
    size comes from the registry. An index present on disk but not in the
    registry is returned with size None and logged, so the gap is visible.
    """
    indices = csv_data_service.list_available_indices()
    missing = universe_registry.missing_entries(indices)
    if missing:
        logger.warning(
            "Indices with no INDEX_SIZE_MAP entry (size returned as null): %s", missing
        )

    result = [describe({"type": ALL, "value": ALL})]
    for name in indices:
        result.append({
            "type": "index",
            "value": name,
            "label": index_label(name),
            "size": universe_registry.get_index_size(name),
        })
    return result


# ── Validation (write paths) ─────────────────────────────────────────────────

def validate_universe(
    universe: Any,
    *,
    max_positions: Optional[int] = None,
    limit: Optional[int] = None,
    claimed_size: Any = _UNSET,
    strict: bool = False,
) -> Optional[int]:
    """
    Validate a universe on a write path and return its registry size.

    Checks, in order:
      1. strict=True only: the index must be in the registry           → 400
      2. A client-sent size must match the registry size, and must be
         null for All Stocks                                           → 422
      3. max_positions / limit must fit inside the universe            → 422

    Checks 2 and 3 are skipped when the size is unknown (All Stocks, or an
    unregistered index) — there is nothing to compare against, and guessing a
    bound would reject valid screeners.

    `strict` defaults to False so that adding an index CSV without a registry
    entry degrades to "no size validation" instead of breaking that index
    outright. Turn it on where an unknown universe should be a hard error.

    Args:
        universe:      dict or UniverseConfig.
        max_positions: rebalance.max_positions, if the payload carries one.
        limit:         row cap from a query param (e.g. /run-adhoc?limit=).
        claimed_size:  size as sent by the client. Omit if the payload has none.

    Raises:
        HTTPException on any failed check.
    """
    uni = as_dict(universe)
    value = uni.get("value")

    if is_all(uni):
        size = None
    else:
        size = universe_registry.get_index_size(value)
        if size is None and strict:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown universe '{value}'. No size is registered for it.",
            )

    if claimed_size is not _UNSET and claimed_size is not None:
        if is_all(uni):
            raise HTTPException(
                status_code=422,
                detail="Universe size must be null for 'All Stocks'.",
            )
        if size is not None and int(claimed_size) != size:
            raise HTTPException(
                status_code=422,
                detail=f"Universe size mismatch: '{value}' holds {size} stocks, "
                       f"but {claimed_size} was sent.",
            )

    if size is not None:
        _check_fits(max_positions, size, value, "max_positions")
        _check_fits(limit, size, value, "limit")

    return size


def _check_fits(requested: Optional[int], size: int, universe_value: Any, field: str) -> None:
    """Reject a stock count larger than the universe it is drawn from."""
    if requested is None:
        return
    if requested > size:
        raise HTTPException(
            status_code=422,
            detail=f"{field} ({requested}) exceeds the universe size — "
                   f"'{universe_value}' holds only {size} stocks.",
        )
