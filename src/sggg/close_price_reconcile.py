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
    if parse_option_contract_key(text):
        return True
    t = normalize_instrument_description(text)
    return bool(
        re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t)
        and re.search(r"\b([PC])\s*\d", t)
    )


_OPTION_COMPACT_RE = re.compile(
    r"^([A-Z0-9][A-Z0-9.]*)\s+(?:(US)\s+)?(\d{1,2}/\d{1,2}/\d{2,4})\s+([PC])\s*(\d+(?:\.\d+)?)\s*(?:US)?\s*$",
    re.IGNORECASE,
)


def _normalize_expiry_mdy(expiry: str) -> str:
    parts = expiry.split("/")
    if len(parts) != 3:
        return expiry
    try:
        m, d, y = (int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        return expiry
    if y < 100:
        y += 2000 if y < 70 else 1900
    return f"{y:04d}-{m:02d}-{d:02d}"


def parse_option_contract_key(text: Any) -> Optional[str]:
    """
    Normalize option lines like SPY 06/18/26 P675 US and SPY US 06/18/26 P702
    to opt:SPY|2026-06-18|P|675.
    """
    t = normalize_instrument_description(text)
    if not t:
        return None
    m = _OPTION_COMPACT_RE.match(t)
    if not m:
        return None
    root, _mkt, expiry, cp, strike = m.groups()
    return f"opt:{root.upper()}|{_normalize_expiry_mdy(expiry)}|{cp.upper()}|{strike}"


def is_cash_position(
    *,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security_name: Any = None,
    security_type: Any = None,
) -> bool:
    for raw in (company_symbol, description, bbg_ticker, security_name):
        u = _norm(raw).upper()
        if not u:
            continue
        if u in ("CASH", "CASH USD", "CASH CAD", "CADUSD", "USDUSD", "USDCAD") or u.startswith(
            "CASH "
        ):
            return True
    st = _norm(security_type).upper()
    return st == "CASH" or st.startswith("CASH ")


def _is_equity_security_type(security_type: Any) -> bool:
    u = _norm(security_type).upper().replace(" ", "")
    if not u:
        return False
    return any(tok in u for tok in ("STOCK", "EQUITY", "ETF", "COMMONSTOCK"))


def is_bond_like_position(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    security_name: Any = None,
    match_key: Any = None,
) -> bool:
    mk = _norm(match_key)
    if mk.startswith("bond:"):
        return True
    if _is_bond_security_type(security_type):
        return True
    for text in (company_symbol, description, security_name):
        if _looks_like_bond_description(text):
            return True
    return False


def is_fund_unit_position(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    security_name: Any = None,
) -> bool:
    """Holdings of another fund share class (e.g. EHF550I vs long fund name in Diamond)."""
    st = _norm(security_type).upper()
    if st and any(tok in st for tok in ("MUTUAL", "FUND", "UNIT TRUST", "LP UNITS")):
        if "BOND" not in st:
            return True
    sym = _norm(company_symbol).upper()
    if sym and re.match(r"^EHF\d+[A-Z]+$", sym):
        return True
    for text in (description, security_name):
        u = _norm(text).upper()
        if u and "ALTERNATIVE FUND" in u:
            return True
    return False


def format_equity_display_ticker(
    *,
    company_symbol: Any = None,
    bbg_ticker: Any = None,
) -> str:
    """Use AlphaDesk COMPANY_SYMBOL as-is (e.g. AMAT.US, ABX.CA)."""
    cs = _norm(company_symbol)
    if cs:
        return cs
    return normalize_bbg_key(bbg_ticker) or _norm(bbg_ticker) or ""


def align_diamond_bond_close(
    diamond_close: Optional[float],
    alphadesk_close: Optional[float],
    *,
    is_bond_like: bool = False,
) -> Optional[float]:
    """Scale Diamond fractional par quotes when AlphaDesk is already in par points."""
    if diamond_close is None:
        return None
    if alphadesk_close is None:
        if is_bond_like and 0 < diamond_close < 20:
            return round(diamond_close * 100.0, 6)
        return diamond_close
    if is_bond_like or (0 < diamond_close < 20 and alphadesk_close > 50):
        if 0 < diamond_close < 20:
            return round(diamond_close * 100.0, 6)
    return diamond_close


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
    description: Any = None,
    is_bond_like: bool = False,
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
    bond_like = is_bond_like or is_bond_like_position(
        security_type=security_type,
        company_symbol=security_name,
        description=description,
        security_name=security_name,
    )
    if bond_like and 0 < p < 20:
        return round(p * 100.0, 6)
    if (
        0 < p < 5
        and not _is_equity_security_type(security_type)
        and not _is_option_security_type(security_type)
    ):
        return round(p * 100.0, 6)
    return round(p, 6)


def reconcile_match_key(
    *,
    company_symbol: Any = None,
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

    Order: ISIN / CUSIP / SEDOL, compact option contract, compact bond line (COMPANY_SYMBOL
    style — same as dashboard portfolio ticker), then equity portfolio line ticker.
    Long issuer names (AlphaDesk DESCRIPTION only) are not used for bonds.
    """
    if is_cash_position(
        company_symbol=company_symbol,
        description=description,
        bbg_ticker=bbg_ticker,
        security_name=security_name,
        security_type=security_type,
    ):
        return None

    iso = _norm(isin).upper()
    if iso:
        return f"isin:{iso}"
    cus = _norm(cusip).upper()
    if cus:
        return f"cusip:{cus}"
    sed = _norm(sedol).upper()
    if sed:
        return f"sedol:{sed}"

    st = _norm(security_type)
    for candidate in (
        company_symbol,
        bbg_ticker,
        security_name,
        description,
    ):
        opt = parse_option_contract_key(candidate)
        if opt:
            return opt

    cs = _norm(company_symbol)
    sn = _norm(security_name)
    desc = _norm(description)
    for bond_line in (cs, sn):
        if _is_bond_security_type(st) or _looks_like_bond_description(bond_line):
            if bond_line and _looks_like_bond_description(bond_line):
                return f"bond:{normalize_instrument_description(bond_line)}"
    if (_is_bond_security_type(st) or _looks_like_bond_description(desc)) and _looks_like_bond_description(desc):
        return f"bond:{normalize_instrument_description(desc)}"

    # Equities / ETFs: dashboard portfolio ticker = company_symbol || bbg_ticker
    line = cs or normalize_bbg_key(bbg_ticker) or normalize_bbg_key(sn)
    if line:
        return f"line:{line}"

    bbg = normalize_bbg_key(bbg_ticker)
    if bbg:
        return f"line:{bbg}"
    return None


def portfolio_line_ticker(
    *,
    company_symbol: Any = None,
    bbg_ticker: Any = None,
    security_name: Any = None,
    security_type: Any = None,
    description: Any = None,
) -> str:
    """Full bond line, option contract, or PSC company_symbol for equities."""
    cs = _norm(company_symbol)
    bbg = _norm(bbg_ticker)
    sn = _norm(security_name)
    desc = _norm(description)

    for candidate in (cs, bbg, sn, desc):
        if parse_option_contract_key(candidate):
            return candidate

    bond_lines = [
        t
        for t in (desc, cs, sn, bbg)
        if t and _looks_like_bond_description(t)
    ]
    if bond_lines:
        return max(bond_lines, key=len)

    if is_fund_unit_position(
        security_type=security_type,
        company_symbol=cs,
        description=desc or sn,
        security_name=sn,
    ):
        return desc or sn or cs or bbg or ""

    equity = format_equity_display_ticker(company_symbol=cs, bbg_ticker=bbg or sn)
    if equity:
        return equity
    return desc or bbg or sn or cs or ""


def pick_display_ticker(
    psc: Optional[Dict[str, Any]],
    dia: Optional[Dict[str, Any]],
) -> str:
    """Prefer PSC company_symbol; descriptive bond line; else best ticker from either side."""
    if psc:
        cs = _norm(psc.get("company_symbol"))
        if cs and not _looks_like_bond_description(cs):
            return cs
    candidates: List[str] = []
    for bucket in (psc, dia):
        if not bucket:
            continue
        for field in ("ticker", "description", "company_symbol", "security_name"):
            val = _norm(bucket.get(field))
            if val:
                candidates.append(val)
    bond_lines = [c for c in candidates if _looks_like_bond_description(c)]
    if bond_lines:
        return max(bond_lines, key=len)
    for bucket in (psc, dia):
        if bucket and bucket.get("ticker"):
            return _norm(bucket["ticker"])
    return candidates[0] if candidates else ""


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
    return reconcile_match_key(
        company_symbol=row.get("company_symbol"),
        bbg_ticker=row.get("bbg_ticker"),
        sedol=row.get("sedol"),
        isin=row.get("isin"),
        cusip=row.get("cusip"),
        description=row.get("description"),
        security_name=row.get("company_symbol"),
        security_type=row.get("security_type"),
    )


def _merge_bucket_into(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    target["shares"] = float(target.get("shares") or 0) + float(source.get("shares") or 0)
    if source.get("close_price") is not None:
        target["close_price"] = source["close_price"]
    for field in ("isin", "cusip", "sedol"):
        if source.get(field) and not target.get(field):
            target[field] = source[field]
    if len(_norm(source.get("ticker"))) > len(_norm(target.get("ticker"))):
        target["ticker"] = source["ticker"]


def _canonical_reconcile_key(*keys: str) -> str:
    """Pick one shared key when PSC and Diamond used different primary keys."""
    order = ("isin:", "cusip:", "sedol:", "opt:", "bond:", "line:")
    for prefix in order:
        for k in keys:
            if k.startswith(prefix):
                return k
    return keys[0]


def _rename_bucket(by_key: Dict[str, Dict[str, Any]], old_key: str, new_key: str) -> None:
    if old_key == new_key:
        return
    if new_key in by_key:
        _merge_bucket_into(by_key[new_key], by_key.pop(old_key))
        by_key[new_key]["match_key"] = new_key
    else:
        by_key[new_key] = by_key.pop(old_key)
        by_key[new_key]["match_key"] = new_key


def merge_positions_by_secondary_ids(
    psc_by_key: Dict[str, Dict[str, Any]],
    dia_by_key: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], int]:
    """
    When primary keys differ but ISIN/CUSIP/SEDOL match, align PSC and Diamond to one key.
    """
    id_to_psc: Dict[str, set] = {}
    id_to_dia: Dict[str, set] = {}

    def _index(side: str, by_key: Dict[str, Dict[str, Any]], dest: Dict[str, set]) -> None:
        for key, bucket in by_key.items():
            for field in ("isin", "cusip", "sedol"):
                val = _norm(bucket.get(field)).upper()
                if val:
                    dest.setdefault(f"{field}:{val}", set()).add(key)

    _index("psc", psc_by_key, id_to_psc)
    _index("dia", dia_by_key, id_to_dia)
    merges = 0

    for idk, psc_keys in id_to_psc.items():
        dia_keys = id_to_dia.get(idk)
        if not dia_keys or len(psc_keys) != 1 or len(dia_keys) != 1:
            continue
        pk = next(iter(psc_keys))
        dk = next(iter(dia_keys))
        if pk == dk:
            continue
        ck = _canonical_reconcile_key(pk, dk)
        _rename_bucket(psc_by_key, pk, ck)
        _rename_bucket(dia_by_key, dk, ck)
        merges += 1

    return psc_by_key, dia_by_key, merges


def _position_notional(bucket: Dict[str, Any]) -> float:
    return abs(
        float(bucket.get("shares") or 0)
        * float(bucket.get("close_price") or 0)
        * float(bucket.get("qty_multiplier") or 1.0)
    )


def merge_fund_unit_holdings_by_navpu(
    psc_by_key: Dict[str, Dict[str, Any]],
    dia_by_key: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], int]:
    """
    Pair fund-in-fund lines when share-class NAVPU and notional are close
    (e.g. EHF550I vs EHP Tactical Growth Alternative Fund Class I).
    """
    psc_only = [k for k in psc_by_key if k not in dia_by_key]
    dia_only = [k for k in dia_by_key if k not in psc_by_key]
    used_dia: set = set()
    merges = 0

    for pk in list(psc_only):
        psc = psc_by_key.get(pk)
        if not psc:
            continue
        if not is_fund_unit_position(
            security_type=psc.get("security_type"),
            company_symbol=psc.get("company_symbol"),
            description=psc.get("description"),
        ):
            continue
        pc = psc.get("close_price")
        if pc is None or float(pc) <= 0:
            continue
        pc_f = float(pc)
        best_dk: Optional[str] = None
        best_rel = 999.0
        for dk in dia_only:
            if dk in used_dia:
                continue
            dia = dia_by_key.get(dk)
            if not dia:
                continue
            if not is_fund_unit_position(
                security_type=dia.get("security_type"),
                company_symbol=dia.get("company_symbol"),
                description=dia.get("description") or dia.get("security_name"),
                security_name=dia.get("security_name"),
            ):
                continue
            dc = dia.get("close_price")
            if dc is None:
                continue
            dc_f = float(dc)
            rel = abs(pc_f - dc_f) / max(pc_f, dc_f)
            if rel > 0.025:
                continue
            pn = _position_notional(psc)
            dn = _position_notional(dia)
            if pn > 1000 and dn > 1000:
                nv_rel = abs(pn - dn) / max(pn, dn)
                if nv_rel > 0.08:
                    continue
            if rel < best_rel:
                best_rel = rel
                best_dk = dk
        if best_dk:
            ck = _canonical_reconcile_key(pk, best_dk)
            _rename_bucket(psc_by_key, pk, ck)
            _rename_bucket(dia_by_key, best_dk, ck)
            used_dia.add(best_dk)
            merges += 1

    return psc_by_key, dia_by_key, merges


def aggregate_psc_by_security(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Roll PSC legs up to one row per reconcile_match_key (net shares, one close)."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if is_cash_position(
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security_type=row.get("security_type"),
        ):
            continue
        key = _psc_match_key(row)
        if not key:
            continue
        signed = _signed_qty(row.get("quantity"), row.get("long_short"))
        if abs(signed) <= 0.0001:
            continue
        mult = notional_quantity_multiplier(
            row.get("security_type"),
            row.get("company_symbol") or row.get("description"),
        )
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": portfolio_line_ticker(
                    company_symbol=row.get("company_symbol"),
                    description=row.get("description"),
                    bbg_ticker=row.get("bbg_ticker"),
                    security_type=row.get("security_type"),
                ),
                "company_symbol": row.get("company_symbol"),
                "description": row.get("description"),
                "shares": 0.0,
                "close_price": row.get("close_price"),
                "qty_multiplier": mult,
                "security_type": row.get("security_type"),
                "isin": row.get("isin"),
                "cusip": row.get("cusip"),
                "sedol": row.get("sedol"),
                "is_bond_like": is_bond_like_position(
                    security_type=row.get("security_type"),
                    company_symbol=row.get("company_symbol"),
                    description=row.get("description"),
                    match_key=key,
                ),
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if row.get("close_price") is not None:
            bucket["close_price"] = row.get("close_price")
        bond_desc = row.get("description")
        if bond_desc and _looks_like_bond_description(bond_desc):
            bucket["description"] = bond_desc
            bucket["ticker"] = bond_desc
        elif not bucket.get("ticker"):
            bucket["ticker"] = portfolio_line_ticker(
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
    if is_cash_position(
        security_name=sec_name,
        bbg_ticker=row.get("PricingTicker"),
        security_type=sec_type,
    ):
        return None
    return reconcile_match_key(
        company_symbol=sec_name,
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
        sec_name = row.get("SecurityName")
        sec_type = row.get("SecurityType") or row.get("AssetType")
        if is_cash_position(
            security_name=sec_name,
            bbg_ticker=row.get("PricingTicker"),
            security_type=sec_type,
        ):
            continue
        key = _diamond_match_key(row)
        if not key:
            continue
        signed = _diamond_signed_qty(row)
        if abs(signed) <= 0.0001:
            continue
        bond_like = is_bond_like_position(
            security_type=sec_type,
            company_symbol=sec_name,
            security_name=sec_name,
            match_key=key,
        )
        close_f = normalize_diamond_close_price(
            row.get("PortfolioPrice"),
            security_name=sec_name,
            security_type=sec_type,
            is_bond_like=bond_like,
        )
        mult = notional_quantity_multiplier(sec_type, sec_name)
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": portfolio_line_ticker(
                    company_symbol=sec_name,
                    bbg_ticker=row.get("PricingTicker"),
                    security_type=sec_type,
                    security_name=sec_name,
                    description=sec_name,
                ),
                "company_symbol": sec_name,
                "security_name": sec_name,
                "description": sec_name,
                "shares": 0.0,
                "close_price": close_f,
                "qty_multiplier": mult,
                "security_type": sec_type,
                "isin": row.get("ISIN"),
                "cusip": row.get("CUSIP"),
                "sedol": row.get("SEDOL"),
                "is_bond_like": bond_like,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if close_f is not None:
            bucket["close_price"] = close_f
        if sec_name and len(_norm(sec_name)) > len(_norm(bucket.get("description"))):
            bucket["description"] = sec_name
            if is_fund_unit_position(security_type=sec_type, security_name=sec_name):
                bucket["ticker"] = sec_name
        if not bucket.get("ticker"):
            bucket["ticker"] = portfolio_line_ticker(
                company_symbol=sec_name,
                bbg_ticker=row.get("PricingTicker"),
                security_type=sec_type,
                security_name=sec_name,
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

    psc_by_key, diamond_by_key, id_merges = merge_positions_by_secondary_ids(
        psc_by_key, diamond_by_key
    )
    meta["secondary_id_merges"] = id_merges
    psc_by_key, diamond_by_key, fund_merges = merge_fund_unit_holdings_by_navpu(
        psc_by_key, diamond_by_key
    )
    meta["fund_unit_merges"] = fund_merges

    all_keys = sorted(set(psc_by_key.keys()) | set(diamond_by_key.keys()))
    lines: List[Dict[str, Any]] = []
    for key in all_keys:
        psc = psc_by_key.get(key)
        dia = diamond_by_key.get(key)
        psc_close = psc.get("close_price") if psc else None
        dia_close_raw = dia.get("close_price") if dia else None
        bond_like = bool(
            (psc and psc.get("is_bond_like"))
            or (dia and dia.get("is_bond_like"))
            or is_bond_like_position(match_key=key)
        )
        dia_close = align_diamond_bond_close(
            dia_close_raw,
            float(psc_close) if psc_close is not None else None,
            is_bond_like=bond_like,
        )
        if dia and dia_close is not None:
            dia = {**dia, "close_price": dia_close}
        price_diff, dollar_diff, shares = _compute_dollar_difference(psc, dia)
        ticker = pick_display_ticker(psc, dia)
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
