"""
Screener Execution Service — runs screener filters/ranking against CSV data.

Data flow:
  1. Get screener DataFrame from csv_data_service (CSV-backed, switchable to DB)
  2. Filter by universe (ALL or index constituents from csv_data_service)
  3. Apply filters as pandas boolean masks
  4. Apply ranking sort
  5. Return paginated results

The translate_field() method dynamically maps UI field names → CSV column names
by reading actual CSV headers at startup. Adding new columns to the CSV files
requires ZERO code changes here — they are auto-discovered.
"""
import logging
import re
from datetime import date
from typing import Dict, List, Optional, Set, Tuple
import json 
import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.filter_registry import FILTER_CONFIG_MAP, get_filter_label, get_db_key
from app.models.screener import ScreenerVersion
from app.services import csv_data_service

logger = logging.getLogger(__name__)


class ScreenerExecutionService:

    def __init__(self):
        # Lazy-loaded: actual CSV column names + reverse lookup
        self._csv_columns: Optional[Set[str]] = None
        self._suffix_index: Optional[Dict[str, List[str]]] = None

    # ── Column aliases now live in app.core.filter_registry (dbKey) ────────────

    # ── CSV column discovery ──────────────────────────────────────────────────

    def _ensure_columns_loaded(self):
        """Read CSV headers once and build a suffix-based reverse index."""
        if self._csv_columns is not None:
            return

        latest_date = csv_data_service.get_latest_screener_date()
        if not latest_date:
            self._csv_columns = set()
            self._suffix_index = {}
            return

        df = csv_data_service.get_screener_data(latest_date)
        self._csv_columns = set(df.columns.tolist())

        # Build suffix index: e.g. "return_pct" → ["1y_return_pct", "9m_return_pct", ...]
        # This lets translate_field("return_pct", "1y") → "1y_return_pct" dynamically.
        self._suffix_index = {}
        period_pattern = re.compile(r'^(\d+[ymwd]?)_(.+)$')

        for col in self._csv_columns:
            m = period_pattern.match(col)
            if m:
                suffix = m.group(2)  # e.g. "return_pct"
                self._suffix_index.setdefault(suffix, []).append(col)

        logger.info(
            "CSV column discovery: %d columns, %d period-suffixed groups",
            len(self._csv_columns), len(self._suffix_index),
        )

    # ── Field mapping ─────────────────────────────────────────────────────────

    def translate_field(self, field: str, period: str = None) -> str:
        """
        Dynamically maps a UI filter field name (+ optional period) to the
        exact CSV column name. Resolution order:

        1. Moving average / EMA special pattern: "200d" → "200_days_ma"
        2. Period-based: field="return_pct", period="1y" → "1y_return_pct"
           (discovered from actual CSV headers, not hardcoded)
        3. UI alias map: "marketcap" → "market_cap_crores"
        4. Direct match: field already matches a CSV column name
        5. Pass-through: return as-is (caller checks if column exists)
        """
        self._ensure_columns_loaded()
        field = (field or "").lower().strip()
        period = (period or "").lower().strip() if period else ""

        # ── Moving average / EMA: period like "200d" → "200_days_ma" ──────
        if field in ("moving_average", "moving_avg"):
            days = period.rstrip("d") if period else "200"
            candidate = f"{days}_days_ma"
            if candidate in self._csv_columns:
                return candidate
            return candidate  # return anyway, caller will check

        if field == "ema":
            days = period.rstrip("d") if period else "200"
            candidate = f"{days}_days_ema"
            if candidate in self._csv_columns:
                return candidate
            return candidate

        # ── Period-based fields: try "{period}_{field}" pattern ────────────
        if period:
            candidate = f"{period}_{field}"
            if candidate in self._csv_columns:
                return candidate
            # Also check suffix index for partial matches
            if field in self._suffix_index:
                for col in self._suffix_index[field]:
                    if col.startswith(f"{period}_"):
                        return col

        # ── Registry dbKey lookup ─────────────────────────────────────────
        db_key = get_db_key(field)
        if db_key:
            return db_key

        # ── Direct CSV column match ───────────────────────────────────────
        if field in self._csv_columns:
            return field

        # ── Pass-through: treat as raw column name ────────────────────────
        return field

    # ── Public execute methods ────────────────────────────────────────────────

    def execute_screener(
        self, db: Session, version_id, limit: int = None, offset: int = 0
    ) -> dict:
        """Run a saved screener version and return results."""
        version = db.query(ScreenerVersion).filter(ScreenerVersion.id == version_id).first()
        if not version:
            raise HTTPException(status_code=404, detail="Screener version not found.")

        results, total_matches, target_date = self._execute_with_params(
            universe_json=version.universe_json,
            filters_json=version.filters_json,
            ranking_json=version.ranking_json,
            limit=limit,
            offset=offset,
        )
        return {
            "screener_id":   version.screener_id,
            "version_id":    version.id,
            "date":          target_date,
            "total_matches": total_matches,
            "results":       results,
        }

    def execute_adhoc(
        self,
        universe: dict,
        filters: List[dict],
        ranking: dict,
        limit: int = None,
        offset: int = 0,
    ) -> dict:
        """Execute an ad-hoc screener (not saved) and return results."""
        results, total_matches, target_date = self._execute_with_params(
            universe_json=universe,
            filters_json=filters,
            ranking_json=ranking,
            limit=limit,
            offset=offset,
        )

        filter_columns = self._extract_filter_columns(filters or [])

        # Resolve the sort column so we can extract sort_value
        sort_col = None
        if ranking:
            sort_col = self.translate_field(
                (ranking.get("field") or ""), ranking.get("period")
            )

        # Only keep filter-selected indicator keys
        keep_keys = {fc["key"] for fc in filter_columns}

        # Reshape results: fixed = symbol + sort_value; dynamic = filter columns only
        slim_results = []
        for r in results:
            full = r.get("indicators") or {}
            # Sort value: check indicators first, then top-level fields
            sort_value = None
            if sort_col:
                sort_value = full.get(sort_col) or r.get(sort_col)

            # Build indicators from both the indicators dict AND top-level
            # core fields (close, volume) that may be referenced by filters
            slim_indicators = {}
            for k in keep_keys:
                if k in full:
                    slim_indicators[k] = full[k]
                elif k in r:
                    slim_indicators[k] = r[k]

            slim_results.append({
                "symbol":     r.get("symbol"),
                "sort_value": sort_value,
                "indicators": slim_indicators,
            })

        return {
            "screener_id":    None,
            "version_id":     None,
            "date":           target_date,
            "total_matches":  total_matches,
            "filter_columns": filter_columns,
            "results":        slim_results,
        }

    # ── Label helpers ──────────────────────────────────────────────────────────
    # Labels & period display now come from app.core.filter_registry

    def _extract_filter_columns(self, filters_json: List[dict]) -> List[dict]:
        """
        Build a deduplicated list of {key, label} for every column
        referenced by the user's active filters. The key is the CSV column
        name; the label is a human-readable header for the table.

        Labels are resolved from the central FILTER_CONFIG_MAP via
        get_filter_label() — single source of truth.
        """
        seen = set()
        columns = []

        def _add(csv_col: str, label: str):
            if csv_col and csv_col not in seen:
                seen.add(csv_col)
                columns.append({"key": csv_col, "label": label})

        for f in filters_json:
            ftype     = f.get("type", "metric_value")
            raw_field = (f.get("field") or "").lower().strip()
            period    = f.get("period")

            if ftype == "relative_level":
                csv_col = self.translate_field(raw_field, period)
                _add(csv_col, get_filter_label(raw_field, period))

            elif ftype == "field_comparison":
                left_field  = (f.get("left_field") or "").lower().strip()
                left_period = f.get("left_period")
                right_field = (f.get("right_field") or "").lower().strip()
                right_period = f.get("right_period")

                l_csv = self.translate_field(left_field, left_period)
                _add(l_csv, get_filter_label(left_field, left_period))

                r_csv = self.translate_field(right_field, right_period)
                _add(r_csv, get_filter_label(right_field, right_period))

            else:
                # metric_value / metric_period_value
                csv_col = self.translate_field(raw_field, period)
                _add(csv_col, get_filter_label(raw_field, period))

        return columns

    def _execute_with_params(
        self,
        universe_json: dict,
        filters_json: List[dict],
        ranking_json: dict,
        limit: int = None,
        offset: int = 0,
        target_date: date = None,
    ) -> Tuple[List[dict], int, Optional[date]]:
        """
        Core execution logic — pure pandas, no DB queries.

        Returns: (results_list, total_matches, effective_date)
        """
        # 1. Resolve target date
        if not target_date:
            target_date = csv_data_service.get_latest_screener_date()
            if not target_date:
                raise HTTPException(
                    status_code=404,
                    detail="No screener CSV data available. Check SCREENER_CSV_DIR setting."
                )

        # 2. Load screener DataFrame for target date
        df = csv_data_service.get_screener_data(target_date)
        if df.empty:
            logger.warning("Screener data empty for date %s", target_date)
            return [], 0, target_date

        # 3. Universe filter
        df = self._apply_universe_filter(df, universe_json, target_date)
        if df.empty:
            return [], 0, target_date

        # 4. Apply metric filters
        df = self._apply_filters(df, filters_json or [])
        if df.empty:
            return [], 0, target_date

        # 5. Apply ranking / sort
        df = self._apply_ranking(df, ranking_json or {})

        # 6. Paginate
        total_matches = len(df)
        if limit is not None:
            df = df.iloc[offset: offset + limit]

        # 7. Format output
        results = self._format_results(df)
        return results, total_matches, target_date

    # ── Private helpers ───────────────────────────────────────────────────────

    def _apply_universe_filter(
        self, df: pd.DataFrame, universe_json: dict, target_date: date
    ) -> pd.DataFrame:
        if not universe_json:
            return df
        uni_type = (universe_json.get("type") or "ALL").upper()
        if uni_type != "INDEX":
            return df  # ALL — no filtering needed

        index_val = universe_json.get("value")
        if not index_val:
            return df

        constituents = csv_data_service.get_index_constituents(index_val, target_date)
        if not constituents:
            logger.warning("No constituents found for index '%s' as of %s", index_val, target_date)
            return df.iloc[0:0]  # empty, preserving columns

        constituents_set = set(constituents)
        return df[df["tradingsymbol"].isin(constituents_set)].copy()

    def _apply_filters(self, df: pd.DataFrame, filters_json: List[dict]) -> pd.DataFrame:
        for f in filters_json:
            try:
                df = self._apply_single_filter(df, f)
            except Exception as exc:
                logger.warning("Skipping invalid filter %s: %s", f, exc)
            if df.empty:
                break
        return df

    def _apply_single_filter(self, df: pd.DataFrame, f: dict) -> pd.DataFrame:
        ftype     = f.get("type", "metric_value")
        operator  = f.get("operator")
        raw_field = (f.get("field") or "").lower().strip()
        period    = f.get("period")
        value     = f.get("value")

        # ── relative_level: price above/below moving average ─────────────────
        if ftype == "relative_level":
            relation = (f.get("relation") or "above").lower()
            ma_col   = self.translate_field(raw_field, period)
            if ma_col not in df.columns or "close" not in df.columns:
                return df  # skip if columns missing
            ma_series    = pd.to_numeric(df[ma_col], errors="coerce")
            close_series = pd.to_numeric(df["close"], errors="coerce")
            mask = close_series > ma_series if relation == "above" else close_series < ma_series
            return df[mask.fillna(False)]

        # ── field_comparison: left_field OP right_field ───────────────────────
        if ftype == "field_comparison":
            left_col  = self.translate_field(f.get("left_field", ""),  f.get("left_period"))
            right_col = self.translate_field(f.get("right_field", ""), f.get("right_period"))
            if left_col not in df.columns or right_col not in df.columns:
                return df
            left_s  = pd.to_numeric(df[left_col],  errors="coerce")
            right_s = pd.to_numeric(df[right_col], errors="coerce")
            return df[self._compare_series(left_s, operator, right_s).fillna(False)]

        # ── standard metric_value ─────────────────────────────────────────────
        db_col = self.translate_field(raw_field, period)
        if db_col not in df.columns:
            logger.debug("Filter column '%s' not found in screener data — skipping", db_col)
            return df
        series = pd.to_numeric(df[db_col], errors="coerce")

        # Coerce value to float — frontend may send numeric values as strings
        if isinstance(value, list):
            value = [float(v) for v in value]
        elif value is not None:
            value = float(value)

        mask   = self._compare_series(series, operator, value)
        return df[mask.fillna(False)]

    @staticmethod
    def _compare_series(series: pd.Series, operator: str, value) -> pd.Series:
        """Apply a comparison operator between a Series and a scalar or another Series."""
        if operator == ">":   return series > value
        if operator == "<":   return series < value
        if operator == ">=":  return series >= value
        if operator == "<=":  return series <= value
        if operator == "==":  return series == value
        if operator == "between" and isinstance(value, list) and len(value) == 2:
            return series.between(value[0], value[1])
        return pd.Series(True, index=series.index)  # unknown operator: pass all through

    def _apply_ranking(self, df: pd.DataFrame, ranking_json: dict) -> pd.DataFrame:
        if not ranking_json:
            return df
        raw_field  = (ranking_json.get("field") or "").lower().strip()
        period     = ranking_json.get("period")
        rank_order = (ranking_json.get("order") or "desc").lower()

        sort_col = self.translate_field(raw_field, period)
        if sort_col not in df.columns:
            logger.debug("Ranking column '%s' not found — returning unsorted", sort_col)
            return df

        ascending = rank_order != "desc"
        numeric   = pd.to_numeric(df[sort_col], errors="coerce")
        df = df.copy()
        df[sort_col] = numeric
        return df.sort_values(sort_col, ascending=ascending, na_position="last")

    @staticmethod
    def _format_results(df: pd.DataFrame) -> List[dict]:
        """Serialize DataFrame rows to API response dicts."""
        core_cols = {"tradingsymbol", "close", "volume"}
        indicator_cols = [c for c in df.columns if c not in core_cols]

        results = []
        for _, row in df.iterrows():
            indicators = {
                col: (None if pd.isna(row[col]) else row[col])
                for col in indicator_cols
            }
            results.append({
                "symbol":     row.get("tradingsymbol"),
                "close":      row.get("close"),
                "volume":     row.get("volume"),
                "indicators": indicators,
            })
        return results


screener_execution_service = ScreenerExecutionService()
