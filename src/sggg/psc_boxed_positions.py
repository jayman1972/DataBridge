"""Detect boxed positions (long + short same security) from AlphaDesk PSC portfolio snapshots."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import (
    NAV_CHECKER_FUND_ID_TO_PSC,
    NAV_CHECKER_PSC_PORTFOLIO_CANDIDATES,
    normalize_valuation_date,
)


# When the requested POSN_DATE_INT has zero rows for a portfolio (e.g. US
# holiday — AlphaDesk doesn't advance the snapshot), look backwards up to this
# many calendar days for the most recent prior snapshot.
_PSC_POSN_DATE_FALLBACK_DAYS = 14


def _normalize_fund_guid(fund_id: str) -> str:
    return (fund_id or "").strip().upper()


def _fund_guid_key(fund_id: str) -> Optional[str]:
    raw = (fund_id or "").strip()
    if not raw:
        return None
    if raw in NAV_CHECKER_FUND_ID_TO_PSC:
        return raw
    key_up = raw.upper()
    for fid in NAV_CHECKER_FUND_ID_TO_PSC:
        if fid.upper() == key_up:
            return fid
    return None


def psc_portfolio_for_fund_id(fund_id: str) -> Optional[str]:
    """Resolve primary PSC portfolio name; fund GUID match is case-insensitive."""
    fid = _fund_guid_key(fund_id)
    if not fid:
        return None
    return NAV_CHECKER_FUND_ID_TO_PSC.get(fid)


def psc_portfolio_candidates_for_fund(fund_id: str) -> List[str]:
    """Ordered PSC portfolio names to try (handles naming variants)."""
    fid = _fund_guid_key(fund_id)
    if not fid:
        return []
    primary = NAV_CHECKER_FUND_ID_TO_PSC.get(fid)
    extras = NAV_CHECKER_PSC_PORTFOLIO_CANDIDATES.get(fid) or []
    out: List[str] = []
    for name in [primary, *extras]:
        n = (name or "").strip()
        if n and n not in out:
            out.append(n)
    return out

# In-memory PSC position rows for follow-on price comparison (same Data Bridge process).
_NAV_CHECKER_PSC_PORTFOLIO: Dict[str, List[Dict[str, Any]]] = {}

_BOX_TYPE_LABELS = {
    "separate_accounts": "Separate accounts (prime broker may show flat)",
    "tag_mismatch": "Different strategy / trade group (AlphaDesk only)",
    "both": "Separate accounts and different strategy / trade group",
}


def get_psc_portfolio_positions(fund_id: str, valuation_date_iso: str) -> List[Dict[str, Any]]:
    """Return cached PSC position rows from the latest NAV checker run (if any)."""
    key = f"{normalize_valuation_date(valuation_date_iso)}:{fund_id.strip()}"
    return list(_NAV_CHECKER_PSC_PORTFOLIO.get(key) or [])


def _compact_date(iso_date: str) -> str:
    return normalize_valuation_date(iso_date).replace("-", "")


def _norm(s: Any) -> str:
    return (str(s or "")).strip()


def _norm_upper(s: Any) -> str:
    return _norm(s).upper()


def _side(long_short: Any) -> Optional[str]:
    u = _norm_upper(long_short)
    if u in ("L", "LONG"):
        return "long"
    if u in ("S", "SHORT"):
        return "short"
    return None


def _security_key(row: Dict[str, Any]) -> str:
    for field in ("bbg_ticker", "sedol", "company_symbol", "security", "description"):
        v = _norm_upper(row.get(field))
        if v:
            return v
    return ""


def _tag_pair(row: Dict[str, Any]) -> Tuple[str, str]:
    return (_norm(row.get("strategy")), _norm(row.get("trade_group")))


def _parse_position_row(
    row: tuple,
) -> Dict[str, Any]:
    return {
        "strategy": _norm(row[0]),
        "trade_group": _norm(row[1]),
        "company_symbol": _norm(row[2]),
        "description": _norm(row[3]),
        "security_type": _norm(row[4]),
        "currency": _norm(row[5]),
        "bbg_ticker": _norm(row[6]),
        "sedol": _norm(row[7]),
        "long_short": _norm(row[8]),
        "quantity": float(row[9]) if row[9] is not None else 0.0,
        "account": _norm(row[10]),
        "account_description": _norm(row[11]),
        "security": _norm(row[12]),
    }


def fetch_psc_positions_for_portfolio(
    cursor: Any,
    portfolio: str,
    posn_date_compact: str,
) -> List[Dict[str, Any]]:
    """Load non-aggregated PSC rows for one fund portfolio on one date."""
    sql = (
        "SELECT ph.STRATEGY, ph.TRADE_GROUP, ph.COMPANY_SYMBOL, ph.DESCRIPTION, ph.SECURITY_TYPE, "
        "ph.SEC_CCY, ph.BBG_TICKER, sd.SEDOL, ph.LONG_SHORT, ph.QUANTITY, "
        "ph.ACCOUNT, ph.ACCOUNT_DESCRIPTION, ph.SECURITY "
        "FROM psc_position_history ph "
        "LEFT JOIN psc_security_data sd ON ph.security_sn = sd.security_sn "
        "WHERE ph.PORTFOLIO = ? AND ph.POSN_DATE_INT = ? "
        "AND ph.QUANTITY IS NOT NULL AND ABS(ph.QUANTITY) > 0.0001"
    )
    sql_like = sql.replace(
        "WHERE ph.PORTFOLIO = ? AND ph.POSN_DATE_INT = ?",
        "WHERE ph.PORTFOLIO LIKE ? AND ph.POSN_DATE_INT = ?",
    )
    cursor.execute(sql, (portfolio, posn_date_compact))
    rows = cursor.fetchall()
    if not rows:
        cursor.execute(sql_like, (f"{portfolio}%", posn_date_compact))
        rows = cursor.fetchall()
    return [_parse_position_row(r) for r in rows]


def _latest_posn_date_for_portfolio(
    cursor: Any,
    portfolio: str,
    requested_date_compact: str,
    *,
    max_lookback_days: int = _PSC_POSN_DATE_FALLBACK_DAYS,
) -> Optional[str]:
    """Find most recent POSN_DATE_INT <= requested for this portfolio (with row data).

    Capped at ``max_lookback_days`` calendar days to avoid stale snapshots.
    Returns the date in compact ``YYYYMMDD`` form, or ``None`` if nothing in
    range. Uses an EXACT portfolio match first, then a LIKE fallback.
    """
    try:
        requested_int = int(requested_date_compact)
        anchor = datetime.strptime(requested_date_compact, "%Y%m%d")
    except (TypeError, ValueError):
        return None
    floor_int = int((anchor - timedelta(days=max_lookback_days)).strftime("%Y%m%d"))
    sql_exact = (
        "SELECT MAX(POSN_DATE_INT) FROM psc_position_history "
        "WHERE PORTFOLIO = ? AND POSN_DATE_INT <= ? AND POSN_DATE_INT >= ? "
        "AND QUANTITY IS NOT NULL AND ABS(QUANTITY) > 0.0001"
    )
    sql_like = sql_exact.replace(
        "WHERE PORTFOLIO = ?", "WHERE PORTFOLIO LIKE ?"
    )
    for sql, port in ((sql_exact, portfolio), (sql_like, f"{portfolio}%")):
        try:
            cursor.execute(sql, (port, requested_int, floor_int))
        except Exception:
            continue
        row = cursor.fetchone()
        if row and row[0] is not None:
            try:
                return f"{int(row[0]):08d}"
            except (TypeError, ValueError):
                continue
    return None


def _fetch_psc_positions_with_fallback(
    cursor: Any,
    portfolio: str,
    posn_date_compact: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """Try exact requested date; fall back to most recent prior snapshot if empty.

    Returns ``(rows, actual_posn_date_compact)``. ``actual_posn_date_compact``
    equals the requested date when no fallback was needed (or when no fallback
    snapshot exists within the lookback window).
    """
    rows = fetch_psc_positions_for_portfolio(cursor, portfolio, posn_date_compact)
    if rows:
        return rows, posn_date_compact
    fallback = _latest_posn_date_for_portfolio(cursor, portfolio, posn_date_compact)
    if fallback and fallback != posn_date_compact:
        fallback_rows = fetch_psc_positions_for_portfolio(cursor, portfolio, fallback)
        if fallback_rows:
            return fallback_rows, fallback
    return [], posn_date_compact


def detect_boxed_positions(positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Find securities with both long and short legs in the same fund snapshot.

    box_type:
      - separate_accounts: long/short in different PSC ACCOUNT values
      - tag_mismatch: different STRATEGY/TRADE_GROUP between long vs short legs
      - both: both conditions
    """
    by_security: Dict[str, List[Dict[str, Any]]] = {}
    for row in positions:
        key = _security_key(row)
        if not key:
            continue
        by_security.setdefault(key, []).append(row)

    boxes: List[Dict[str, Any]] = []
    for key, legs in by_security.items():
        long_legs: List[Dict[str, Any]] = []
        short_legs: List[Dict[str, Any]] = []
        for leg in legs:
            side = _side(leg.get("long_short"))
            if side == "long":
                long_legs.append(leg)
            elif side == "short":
                short_legs.append(leg)

        long_qty = sum(abs(float(leg.get("quantity") or 0)) for leg in long_legs)
        short_qty = sum(abs(float(leg.get("quantity") or 0)) for leg in short_legs)
        if long_qty <= 0.0001 or short_qty <= 0.0001:
            continue

        long_accounts = {_norm(leg.get("account")) for leg in long_legs if _norm(leg.get("account"))}
        short_accounts = {_norm(leg.get("account")) for leg in short_legs if _norm(leg.get("account"))}
        long_tags = {_tag_pair(leg) for leg in long_legs}
        short_tags = {_tag_pair(leg) for leg in short_legs}

        separate = bool(long_accounts and short_accounts and long_accounts != short_accounts)
        tag_diff = long_tags != short_tags
        if not separate and not tag_diff:
            continue

        if separate and tag_diff:
            box_type = "both"
        elif separate:
            box_type = "separate_accounts"
        else:
            box_type = "tag_mismatch"

        sample = long_legs[0] if long_legs else short_legs[0]
        ad_symbol = _norm(sample.get("company_symbol")) or _norm(sample.get("security"))
        boxes.append(
            {
                "security_key": key,
                "company_symbol": ad_symbol or None,
                "description": sample.get("description") or sample.get("company_symbol"),
                "bbg_ticker": sample.get("bbg_ticker"),
                "sedol": sample.get("sedol"),
                "box_type": box_type,
                "box_type_label": _BOX_TYPE_LABELS.get(box_type, box_type),
                "long_quantity": round(long_qty, 4),
                "short_quantity": round(short_qty, 4),
                "long_legs": _summarize_legs(long_legs),
                "short_legs": _summarize_legs(short_legs),
            }
        )

    boxes.sort(key=lambda b: (b.get("description") or b.get("security_key") or "").upper())
    return boxes


def _summarize_legs(legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for leg in legs:
        out.append(
            {
                "strategy": leg.get("strategy"),
                "trade_group": leg.get("trade_group"),
                "account": leg.get("account"),
                "account_description": leg.get("account_description"),
                "quantity": leg.get("quantity"),
            }
        )
    return out


def _compact_to_iso(compact: Optional[str]) -> Optional[str]:
    if not compact or len(compact) != 8:
        return None
    return f"{compact[0:4]}-{compact[4:6]}-{compact[6:8]}"


def fetch_boxed_positions_for_funds(
    fund_specs: List[Dict[str, str]],
    valuation_date_iso: str,
    *,
    store_portfolios: bool = True,
    dsn: str = "PSC_VIEWER",
) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Optional[str],
    Dict[str, str],
]:
    """
    Query PSC for each fund and return boxed positions keyed by fund_id.

    Also caches full position lists for later AlphaDesk vs SGGG price comparison.

    Returns ``(boxed_by_fund, positions_by_fund, fund_error, actual_posn_dates_by_fund)``
    where ``actual_posn_dates_by_fund`` maps fund_id -> ISO date (YYYY-MM-DD)
    actually used in PSC; entries are present only when PSC fell back to a
    prior snapshot date (e.g. on US holidays AlphaDesk doesn't advance
    POSN_DATE_INT).
    """
    try:
        import pyodbc
    except ImportError:
        return {}, {}, "pyodbc not installed", {}

    requested_date_compact = _compact_date(valuation_date_iso)
    boxed_by_fund: Dict[str, List[Dict[str, Any]]] = {}
    positions_by_fund: Dict[str, List[Dict[str, Any]]] = {}
    actual_posn_dates_by_fund: Dict[str, str] = {}
    fund_error: Optional[str] = None
    conn = None
    try:
        conn = pyodbc.connect(f"DSN={dsn}")
        cursor = conn.cursor()
        for spec in fund_specs:
            fid = (spec.get("id") or spec.get("fund_id") or "").strip()
            if not fid:
                continue
            portfolio = psc_portfolio_for_fund_id(fid)
            if not portfolio:
                boxed_by_fund[fid] = []
                positions_by_fund[fid] = []
                continue
            try:
                positions, actual_compact = _fetch_psc_positions_with_fallback(
                    cursor, portfolio, requested_date_compact
                )
            except Exception as leg_exc:
                positions = []
                positions_by_fund[fid] = positions
                boxed_by_fund[fid] = []
                if fund_error is None:
                    fund_error = f"{portfolio}: {leg_exc}"
                continue
            positions_by_fund[fid] = positions
            boxed_by_fund[fid] = detect_boxed_positions(positions)
            if positions and actual_compact and actual_compact != requested_date_compact:
                iso = _compact_to_iso(actual_compact)
                if iso:
                    actual_posn_dates_by_fund[fid] = iso
            if store_portfolios:
                _NAV_CHECKER_PSC_PORTFOLIO[f"{normalize_valuation_date(valuation_date_iso)}:{fid}"] = (
                    positions
                )
    except Exception as exc:
        return boxed_by_fund, positions_by_fund, str(exc), actual_posn_dates_by_fund
    finally:
        if conn:
            conn.close()
    return boxed_by_fund, positions_by_fund, fund_error, actual_posn_dates_by_fund
