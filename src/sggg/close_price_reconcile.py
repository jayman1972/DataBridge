"""Line-by-line closing price reconciliation: Diamond GetPortfolio vs AlphaDesk PSC."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import normalize_valuation_date
from sggg.psc_boxed_positions import (
    _norm,
    _side,
    psc_portfolio_for_fund_id,
)


def normalize_bbg_key(ticker: Any) -> str:
    """Normalize Bloomberg tickers for cross-system matching."""
    s = _norm(ticker).upper()
    if not s:
        return ""
    if s.endswith(" EQUITY"):
        s = s[: -len(" EQUITY")].strip()
    return re.sub(r"\s+", " ", s)


def reconcile_match_key(
    *,
    bbg_ticker: Any = None,
    sedol: Any = None,
    isin: Any = None,
    cusip: Any = None,
) -> Optional[str]:
    """
    Shared security key for Diamond vs PSC.

    Priority: normalized BBG ticker, SEDOL, ISIN, CUSIP (same identifiers both systems carry).
    """
    bbg = normalize_bbg_key(bbg_ticker)
    if bbg:
        return f"bbg:{bbg}"
    sed = _norm(sedol).upper()
    if sed:
        return f"sedol:{sed}"
    iso = _norm(isin).upper()
    if iso:
        return f"isin:{iso}"
    cus = _norm(cusip).upper()
    if cus:
        return f"cusip:{cus}"
    return None


def display_ticker(
    *,
    company_symbol: Any = None,
    bbg_ticker: Any = None,
    sedol: Any = None,
) -> str:
    """Prefer AlphaDesk COMPANY_SYMBOL (e.g. HYG.US) for display."""
    sym = _norm(company_symbol)
    if sym:
        return sym
    bbg = _norm(bbg_ticker)
    if bbg:
        return bbg
    sed = _norm(sedol)
    if sed:
        return sed
    return ""


def _signed_qty(quantity: Any, long_short: Any) -> float:
    q = abs(float(quantity or 0))
    if q <= 0.0001:
        return 0.0
    if _side(long_short) == "short":
        return -q
    return q


def _parse_psc_reconcile_row(row: tuple) -> Dict[str, Any]:
    return {
        "company_symbol": _norm(row[0]),
        "bbg_ticker": _norm(row[1]),
        "isin": _norm(row[2]),
        "cusip": _norm(row[3]),
        "sedol": _norm(row[4]),
        "long_short": _norm(row[5]),
        "quantity": float(row[6]) if row[6] is not None else 0.0,
        "close_price": float(row[7]) if row[7] is not None else None,
    }


def fetch_psc_positions_for_reconcile(
    cursor: Any,
    portfolio: str,
    posn_date_compact: str,
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT ph.COMPANY_SYMBOL, ph.BBG_TICKER, ph.ISIN, ph.CUSIP, sd.SEDOL, "
        "ph.LONG_SHORT, ph.QUANTITY, ph.CLOSE_PRICE "
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
    return [_parse_psc_reconcile_row(r) for r in rows]


def aggregate_psc_by_security(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Roll PSC legs up to one row per reconcile_match_key (net shares, one close)."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = reconcile_match_key(
            bbg_ticker=row.get("bbg_ticker"),
            sedol=row.get("sedol"),
            isin=row.get("isin"),
            cusip=row.get("cusip"),
        )
        if not key:
            continue
        signed = _signed_qty(row.get("quantity"), row.get("long_short"))
        if abs(signed) <= 0.0001:
            continue
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": display_ticker(
                    company_symbol=row.get("company_symbol"),
                    bbg_ticker=row.get("bbg_ticker"),
                    sedol=row.get("sedol"),
                ),
                "company_symbol": row.get("company_symbol"),
                "bbg_ticker": row.get("bbg_ticker"),
                "shares": 0.0,
                "close_price": row.get("close_price"),
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if row.get("close_price") is not None:
            bucket["close_price"] = row.get("close_price")
        if not bucket.get("ticker"):
            bucket["ticker"] = display_ticker(
                company_symbol=row.get("company_symbol"),
                bbg_ticker=row.get("bbg_ticker"),
            )
    return out


def flatten_diamond_portfolio_records(raw: Any) -> List[Dict[str, Any]]:
    """Extract flat list of PortfolioRecordDetails dicts from GetPortfolio JSON."""
    if not isinstance(raw, dict):
        return []
    body = raw.get("GetPortfolioResponse") or raw
    if not isinstance(body, dict):
        return []
    details = body.get("PortfolioRecordDetails") or []
    if isinstance(details, dict):
        details = [details]
    flat: List[Dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            if "PortfolioPrice" in node or "PricingTicker" in node or "Quantity" in node:
                flat.append(node)
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(details)
    if flat:
        return flat
    if isinstance(details, list):
        return [d for d in details if isinstance(d, dict)]
    return []


def _diamond_signed_qty(row: Dict[str, Any]) -> float:
    q = row.get("Quantity")
    try:
        qty = float(q) if q is not None else 0.0
    except (TypeError, ValueError):
        return 0.0
    if abs(qty) <= 0.0001:
        return 0.0
    ls = _norm(row.get("LongShort")).upper()
    if ls in ("S", "SHORT") and qty > 0:
        return -qty
    return qty


def _normalize_quote_date(val: Any, valuation_date_iso: str) -> bool:
    """True if row QuoteDate matches valuation date (or QuoteDate missing)."""
    quote = _norm(val)
    if not quote:
        return True
    val = normalize_valuation_date(valuation_date_iso)
    if quote == val:
        return True
    if len(quote) >= 10 and quote[:10] == val:
        return True
    compact = quote.replace("-", "")[:8]
    val_compact = val.replace("-", "")
    return compact == val_compact


def aggregate_diamond_by_security(
    records: List[Dict[str, Any]],
    valuation_date_iso: str,
) -> Dict[str, Dict[str, Any]]:
    """Roll Diamond holdings to one row per reconcile_match_key."""
    out: Dict[str, Dict[str, Any]] = {}
    dated = [r for r in records if _normalize_quote_date(r.get("QuoteDate"), valuation_date_iso)]
    use_rows = dated if dated else records
    for row in use_rows:
        key = reconcile_match_key(
            bbg_ticker=row.get("PricingTicker"),
            sedol=row.get("SEDOL"),
            isin=row.get("ISIN"),
            cusip=row.get("CUSIP"),
        )
        if not key:
            continue
        signed = _diamond_signed_qty(row)
        if abs(signed) <= 0.0001:
            continue
        close = row.get("PortfolioPrice")
        try:
            close_f = float(close) if close is not None else None
        except (TypeError, ValueError):
            close_f = None
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": _norm(row.get("PricingTicker")) or _norm(row.get("SecurityName")),
                "bbg_ticker": _norm(row.get("PricingTicker")),
                "shares": 0.0,
                "close_price": close_f,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if close_f is not None:
            bucket["close_price"] = close_f
    return out


def build_close_price_reconciliation(
    fund_id: str,
    valuation_date_iso: str,
    diamond_raw: Any,
    *,
    dsn: str = "PSC_VIEWER",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Compare EOD close prices for all matched securities.

    dollar_difference = net_shares * (diamond_close - alphadesk_close)
    using net shares from PSC when present, else Diamond.
    """
    meta: Dict[str, Any] = {
        "fund_id": fund_id,
        "valuation_date": normalize_valuation_date(valuation_date_iso),
        "psc_error": None,
        "diamond_positions": 0,
        "psc_positions": 0,
        "matched_securities": 0,
    }
    portfolio = psc_portfolio_for_fund_id(fund_id)
    psc_by_key: Dict[str, Dict[str, Any]] = {}
    if not portfolio:
        meta["psc_error"] = f"No PSC portfolio mapping for fund {fund_id}"
    else:
        try:
            import pyodbc
        except ImportError:
            meta["psc_error"] = "pyodbc not installed"
        else:
            date_compact = normalize_valuation_date(valuation_date_iso).replace("-", "")
            try:
                conn = pyodbc.connect(f"DSN={dsn}")
                try:
                    cursor = conn.cursor()
                    psc_rows = fetch_psc_positions_for_reconcile(cursor, portfolio, date_compact)
                    meta["psc_positions"] = len(psc_rows)
                    psc_by_key = aggregate_psc_by_security(psc_rows)
                finally:
                    conn.close()
            except Exception as exc:
                meta["psc_error"] = str(exc)

    diamond_records = flatten_diamond_portfolio_records(diamond_raw)
    meta["diamond_positions"] = len(diamond_records)
    diamond_by_key = aggregate_diamond_by_security(diamond_records, valuation_date_iso)
    if diamond_records and not diamond_by_key:
        meta["diamond_date_warning"] = (
            "No Diamond rows matched valuation date on QuoteDate; check GetPortfolio response."
        )

    all_keys = sorted(set(psc_by_key.keys()) | set(diamond_by_key.keys()))
    lines: List[Dict[str, Any]] = []
    for key in all_keys:
        psc = psc_by_key.get(key)
        dia = diamond_by_key.get(key)
        psc_close = psc.get("close_price") if psc else None
        dia_close = dia.get("close_price") if dia else None
        ticker = ""
        if psc:
            ticker = psc.get("ticker") or display_ticker(
                company_symbol=psc.get("company_symbol"),
                bbg_ticker=psc.get("bbg_ticker"),
            )
        if not ticker and dia:
            ticker = dia.get("ticker") or dia.get("bbg_ticker") or ""
        shares = 0.0
        if psc and abs(float(psc.get("shares") or 0)) > 0.0001:
            shares = float(psc["shares"])
        elif dia:
            shares = float(dia.get("shares") or 0)
        price_diff: Optional[float] = None
        dollar_diff: Optional[float] = None
        if psc_close is not None and dia_close is not None:
            price_diff = round(dia_close - psc_close, 6)
            dollar_diff = round(shares * price_diff, 2)
        lines.append(
            {
                "match_key": key,
                "ticker": ticker or key,
                "diamond_close": dia_close,
                "alphadesk_close": psc_close,
                "price_difference": price_diff,
                "shares": round(shares, 4) if abs(shares) > 0.0001 else 0.0,
                "dollar_difference": dollar_diff,
                "in_diamond": dia is not None,
                "in_alphadesk": psc is not None,
            }
        )

    lines.sort(
        key=lambda r: (
            -abs(float(r["dollar_difference"] or 0)),
            (r.get("ticker") or "").upper(),
        ),
    )
    meta["matched_securities"] = sum(
        1 for r in lines if r.get("diamond_close") is not None and r.get("alphadesk_close") is not None
    )
    meta["lines_with_price_diff"] = sum(
        1
        for r in lines
        if r.get("price_difference") is not None and abs(float(r["price_difference"])) > 0.0001
    )
    meta["total_dollar_difference"] = round(
        sum(float(r["dollar_difference"] or 0) for r in lines if r.get("dollar_difference") is not None),
        2,
    )
    return lines, meta


def fetch_close_price_reconciliation(
    fund_id: str,
    valuation_date_iso: str,
    diamond_client: Any,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """Load Diamond portfolio and PSC, return reconciliation lines."""
    try:
        raw = diamond_client.get_portfolio(
            fund_id=fund_id,
            valuation_date=normalize_valuation_date(valuation_date_iso),
        )
    except Exception as exc:
        return [], {"fund_id": fund_id, "valuation_date": valuation_date_iso}, str(exc)
    lines, meta = build_close_price_reconciliation(fund_id, valuation_date_iso, raw)
    meta["fund_id"] = fund_id
    meta["valuation_date"] = normalize_valuation_date(valuation_date_iso)
    return lines, meta, None
