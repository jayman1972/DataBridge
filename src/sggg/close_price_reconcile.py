"""Line-by-line closing price reconciliation: Diamond GetPortfolio vs AlphaDesk PSC."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import normalize_valuation_date
from sggg.psc_boxed_positions import (
    _norm,
    _side,
    psc_portfolio_candidates_for_fund,
)


# Bond description patterns: "AAL 5 3/4 04/20/29", "AAP 7 08/01/30"
_BOND_DESC_RE = re.compile(
    r"\d+\s+\d+/\d+|\d{1,2}/\d{1,2}/\d{2,4}",
    re.IGNORECASE,
)


def normalize_bbg_key(ticker: Any) -> str:
    """Normalize Bloomberg tickers for cross-system matching."""
    s = _norm(ticker).upper()
    if not s:
        return ""
    if s.endswith(" EQUITY"):
        s = s[: -len(" EQUITY")].strip()
    if s.endswith(" CORP"):
        s = s[: -len(" CORP")].strip()
    return re.sub(r"\s+", " ", s)


def normalize_instrument_description(desc: Any) -> str:
    """Collapse whitespace for bond/option description matching."""
    s = _norm(desc).upper()
    return re.sub(r"\s+", " ", s)


def _is_bond_security_type(security_type: Any) -> bool:
    u = _norm(security_type).upper()
    if not u:
        return False
    return any(tok in u for tok in ("BOND", "NOTE", "DEBENTURE", "FIXED INCOME", "FIXEDINCOME"))


def _is_option_security_type(security_type: Any) -> bool:
    u = _norm(security_type).upper().replace(" ", "")
    return "OPTION" in u or u == "EQUITYOPTION"


def _looks_like_bond_description(text: Any) -> bool:
    t = normalize_instrument_description(text)
    if not t:
        return False
    if _BOND_DESC_RE.search(t):
        return True
    return bool(re.search(r"\d+\s+\d+/\d+", t))


def _looks_like_option_description(text: Any) -> bool:
    t = normalize_instrument_description(text)
    if not t:
        return False
    return bool(
        re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t)
        and re.search(r"\b[CP]\d", t)
    )


def notional_quantity_multiplier(security_type: Any, description: Any = None) -> float:
    """Options: contracts × 100 for dollar impact."""
    if _is_option_security_type(security_type) or _looks_like_option_description(description):
        return 100.0
    return 1.0


def normalize_diamond_close_price(
    price: Any,
    *,
    security_name: Any = None,
    security_type: Any = None,
) -> Optional[float]:
    """
    Diamond reports bond prices as fraction of par (0.99982); AlphaDesk uses 99.982.
    """
    if price is None:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    name = _norm(security_name)
    if (_is_bond_security_type(security_type) or _looks_like_bond_description(name)) and 0 < p < 20:
        return round(p * 100.0, 6)
    return round(p, 6)


def reconcile_match_key(
    *,
    bbg_ticker: Any = None,
    sedol: Any = None,
    isin: Any = None,
    cusip: Any = None,
    description: Any = None,
    security_name: Any = None,
    security_type: Any = None,
) -> Optional[str]:
    """
    Shared security key for Diamond vs PSC.

    Bonds/options: normalized DESCRIPTION / SecurityName (e.g. "AAL 5 3/4 04/20/29").
    Equities: Bloomberg / SEDOL / ISIN / CUSIP.
    """
    desc = normalize_instrument_description(description or security_name)
    st = _norm(security_type)

    if _is_option_security_type(st) or _looks_like_option_description(desc):
        if desc:
            return f"desc:{desc}"

    if _is_bond_security_type(st) or _looks_like_bond_description(desc):
        if desc:
            return f"desc:{desc}"

    bbg = normalize_bbg_key(bbg_ticker)
    if bbg and not _looks_like_bond_description(bbg):
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
    if desc:
        return f"desc:{desc}"
    return None


def display_ticker(
    *,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security_type: Any = None,
    security_name: Any = None,
) -> str:
    """Prefer DESCRIPTION for bonds/options; else AlphaDesk COMPANY_SYMBOL."""
    desc = _norm(description) or _norm(security_name)
    if desc and (
        _is_bond_security_type(security_type)
        or _is_option_security_type(security_type)
        or _looks_like_bond_description(desc)
        or _looks_like_option_description(desc)
    ):
        return desc
    sym = _norm(company_symbol)
    if sym:
        return sym
    bbg = _norm(bbg_ticker)
    if bbg:
        return bbg
    return desc


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
        "description": _norm(row[1]),
        "bbg_ticker": _norm(row[2]),
        "isin": _norm(row[3]),
        "cusip": _norm(row[4]),
        "sedol": _norm(row[5]),
        "security_type": _norm(row[6]),
        "long_short": _norm(row[7]),
        "quantity": float(row[8]) if row[8] is not None else 0.0,
        "close_price": float(row[9]) if row[9] is not None else None,
    }


def fetch_psc_positions_for_reconcile(
    cursor: Any,
    portfolio: str,
    posn_date_compact: str,
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT ph.COMPANY_SYMBOL, ph.DESCRIPTION, ph.BBG_TICKER, ph.ISIN, ph.CUSIP, sd.SEDOL, "
        "ph.SECURITY_TYPE, ph.LONG_SHORT, ph.QUANTITY, ph.CLOSE_PRICE "
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


def fetch_psc_positions_for_fund(
    cursor: Any,
    fund_id: str,
    posn_date_compact: str,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Try each known PSC portfolio alias until rows are found."""
    for portfolio in psc_portfolio_candidates_for_fund(fund_id):
        rows = fetch_psc_positions_for_reconcile(cursor, portfolio, posn_date_compact)
        if rows:
            return rows, portfolio
    candidates = psc_portfolio_candidates_for_fund(fund_id)
    return [], (candidates[0] if candidates else None)


def _psc_match_key(row: Dict[str, Any]) -> Optional[str]:
    desc = row.get("description") or row.get("company_symbol")
    return reconcile_match_key(
        bbg_ticker=row.get("bbg_ticker"),
        sedol=row.get("sedol"),
        isin=row.get("isin"),
        cusip=row.get("cusip"),
        description=desc,
        security_name=desc,
        security_type=row.get("security_type"),
    )


def aggregate_psc_by_security(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Roll PSC legs up to one row per reconcile_match_key (net shares, one close)."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = _psc_match_key(row)
        if not key:
            continue
        signed = _signed_qty(row.get("quantity"), row.get("long_short"))
        if abs(signed) <= 0.0001:
            continue
        mult = notional_quantity_multiplier(row.get("security_type"), row.get("description"))
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": display_ticker(
                    company_symbol=row.get("company_symbol"),
                    description=row.get("description"),
                    bbg_ticker=row.get("bbg_ticker"),
                    security_type=row.get("security_type"),
                ),
                "shares": 0.0,
                "close_price": row.get("close_price"),
                "qty_multiplier": mult,
                "security_type": row.get("security_type"),
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if row.get("close_price") is not None:
            bucket["close_price"] = row.get("close_price")
        if not bucket.get("ticker"):
            bucket["ticker"] = display_ticker(
                company_symbol=row.get("company_symbol"),
                description=row.get("description"),
                bbg_ticker=row.get("bbg_ticker"),
                security_type=row.get("security_type"),
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


def _diamond_match_key(row: Dict[str, Any]) -> Optional[str]:
    sec_name = row.get("SecurityName")
    sec_type = row.get("SecurityType") or row.get("AssetType")
    return reconcile_match_key(
        bbg_ticker=row.get("PricingTicker"),
        sedol=row.get("SEDOL"),
        isin=row.get("ISIN"),
        cusip=row.get("CUSIP"),
        description=sec_name,
        security_name=sec_name,
        security_type=sec_type,
    )


def aggregate_diamond_by_security(
    records: List[Dict[str, Any]],
    valuation_date_iso: str,
) -> Dict[str, Dict[str, Any]]:
    """Roll Diamond holdings to one row per reconcile_match_key."""
    out: Dict[str, Dict[str, Any]] = {}
    dated = [r for r in records if _normalize_quote_date(r.get("QuoteDate"), valuation_date_iso)]
    use_rows = dated if dated else records
    for row in use_rows:
        key = _diamond_match_key(row)
        if not key:
            continue
        signed = _diamond_signed_qty(row)
        if abs(signed) <= 0.0001:
            continue
        sec_name = row.get("SecurityName")
        sec_type = row.get("SecurityType") or row.get("AssetType")
        close_f = normalize_diamond_close_price(
            row.get("PortfolioPrice"),
            security_name=sec_name,
            security_type=sec_type,
        )
        mult = notional_quantity_multiplier(sec_type, sec_name)
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": display_ticker(
                    description=sec_name,
                    bbg_ticker=row.get("PricingTicker"),
                    security_type=sec_type,
                    security_name=sec_name,
                ),
                "shares": 0.0,
                "close_price": close_f,
                "qty_multiplier": mult,
                "security_type": sec_type,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if close_f is not None:
            bucket["close_price"] = close_f
        if not bucket.get("ticker"):
            bucket["ticker"] = display_ticker(
                description=sec_name,
                bbg_ticker=row.get("PricingTicker"),
                security_type=sec_type,
            )
    return out


def _compute_dollar_difference(
    psc: Optional[Dict[str, Any]],
    dia: Optional[Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float], float]:
    """
    Returns (price_difference, dollar_difference, shares_for_display).

    Both sides: net_shares × mult × (diamond_close - alphadesk_close).
    One side only: full position value (signed qty × mult × that side's close).
    """
    psc_close = psc.get("close_price") if psc else None
    dia_close = dia.get("close_price") if dia else None
    shares = 0.0
    mult = 1.0
    if psc and abs(float(psc.get("shares") or 0)) > 0.0001:
        shares = float(psc["shares"])
        mult = float(psc.get("qty_multiplier") or 1.0)
    elif dia:
        shares = float(dia.get("shares") or 0)
        mult = float(dia.get("qty_multiplier") or 1.0)

    if psc_close is not None and dia_close is not None:
        price_diff = round(dia_close - psc_close, 6)
        dollar_diff = round(shares * mult * price_diff, 2)
        return price_diff, dollar_diff, shares

    if psc_close is not None and dia_close is None and psc:
        dollar_diff = round(float(psc["shares"]) * float(psc.get("qty_multiplier") or 1.0) * psc_close, 2)
        return None, dollar_diff, float(psc["shares"])

    if dia_close is not None and psc_close is None and dia:
        dollar_diff = round(float(dia["shares"]) * float(dia.get("qty_multiplier") or 1.0) * dia_close, 2)
        return None, dollar_diff, float(dia["shares"])

    return None, None, shares


def build_close_price_reconciliation(
    fund_id: str,
    valuation_date_iso: str,
    diamond_raw: Any,
    *,
    dsn: str = "PSC_VIEWER",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Compare EOD close prices for all matched securities.
    """
    meta: Dict[str, Any] = {
        "fund_id": fund_id,
        "valuation_date": normalize_valuation_date(valuation_date_iso),
        "psc_error": None,
        "psc_portfolio": None,
        "diamond_positions": 0,
        "psc_positions": 0,
        "matched_securities": 0,
    }
    psc_by_key: Dict[str, Dict[str, Any]] = {}
    candidates = psc_portfolio_candidates_for_fund(fund_id)
    if not candidates:
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
                    psc_rows, portfolio_used = fetch_psc_positions_for_fund(
                        cursor, fund_id, date_compact
                    )
                    meta["psc_portfolio"] = portfolio_used
                    meta["psc_positions"] = len(psc_rows)
                    if not psc_rows and portfolio_used:
                        meta["psc_error"] = (
                            f"No PSC rows for {portfolio_used} on {valuation_date_iso} "
                            f"(tried: {', '.join(candidates)})"
                        )
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
        price_diff, dollar_diff, shares = _compute_dollar_difference(psc, dia)
        ticker = ""
        if psc:
            ticker = psc.get("ticker") or ""
        if not ticker and dia:
            ticker = dia.get("ticker") or ""
        lines.append(
            {
                "match_key": key,
                "ticker": ticker or key,
                "diamond_close": dia.get("close_price") if dia else None,
                "alphadesk_close": psc.get("close_price") if psc else None,
                "price_difference": price_diff,
                "shares": round(shares, 4) if abs(shares) > 0.0001 else 0.0,
                "dollar_difference": dollar_diff,
                "in_diamond": dia is not None,
                "in_alphadesk": psc is not None,
                "one_sided": (psc is None) != (dia is None),
            }
        )

    lines.sort(
        key=lambda r: (
            -abs(float(r["dollar_difference"] or 0)),
            (r.get("ticker") or "").upper(),
        ),
    )
    meta["matched_securities"] = sum(
        1
        for r in lines
        if r.get("diamond_close") is not None and r.get("alphadesk_close") is not None
    )
    meta["lines_with_price_diff"] = sum(
        1
        for r in lines
        if r.get("price_difference") is not None and abs(float(r["price_difference"])) > 0.0001
    )
    meta["one_sided_lines"] = sum(1 for r in lines if r.get("one_sided"))
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
