"""Line-by-line closing price reconciliation: Diamond GetPortfolio vs AlphaDesk PSC."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import normalize_valuation_date, prior_business_day_iso
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


def _is_futures_security_type(security_type: Any) -> bool:
    u = _norm(security_type).upper().replace(" ", "")
    return u == "FUTURES" or u == "FUTURE" or u.startswith("FUTURES")


def _is_plausible_cusip(cusip: Any) -> bool:
    """True for standard 9-character CUSIPs (not futures tickers like NQU6)."""
    c = _norm(cusip).upper()
    return len(c) == 9 and bool(re.fullmatch(r"[A-Z0-9]{9}", c))


def _is_plausible_sedol(sedol: Any) -> bool:
    """True for standard 7-character SEDOLs."""
    s = _norm(sedol).upper()
    return len(s) == 7 and bool(re.fullmatch(r"[A-Z0-9]{7}", s))


def _futures_year_from_digits(year_digits: str) -> int:
    y = int(year_digits)
    if y >= 100:
        return y
    # Bloomberg month codes use a single trailing digit for the current decade (NQM6 = Jun 2026).
    if len(year_digits) == 1:
        return 2020 + y
    return 2000 + y if y < 70 else 1900 + y


def _parse_bloomberg_futures_parts(text: Any) -> Optional[Tuple[str, int, int]]:
    """Parse NQM6 US / ESM5 Index -> (root, year, month)."""
    raw = normalize_bbg_key(text).upper()
    if not raw:
        return None
    token = _FUTURES_BBG_SUFFIX_RE.sub("", raw.split()[0])
    # Non-greedy root: NQM6 -> NQ + M + 6, not NQM + M + 6.
    m = re.match(r"^(.+?)([FGHJKMNQUVXZ])(\d{1,2})$", token)
    if not m:
        return None
    root, month_code, year_digits = m.groups()
    month = _FUTURES_MONTH_CODE.get(month_code.upper())
    if not month or not root:
        return None
    return root.upper(), _futures_year_from_digits(year_digits), month


def _parse_futures_expiry_from_description(text: Any) -> Optional[Tuple[int, int]]:
    """Parse 'NASDAQ 100 E-MINI Jun26' / 'SPI 200 FUTURES Mar19' -> (year, month)."""
    t = normalize_instrument_description(text)
    if not t:
        return None
    m = _FUTURES_DESC_EXPIRY_RE.search(t)
    if not m:
        return None
    mon_token, year_digits = m.groups()
    month = _MONTH_TO_NUM.get(mon_token.upper()[:3])
    if not month:
        return None
    return _futures_year_from_digits(year_digits), month


def _futures_product_root_from_text(text: Any) -> Optional[str]:
    t = normalize_instrument_description(text)
    if not t:
        return None
    for phrase, root in sorted(_FUTURES_PRODUCT_ROOT.items(), key=lambda kv: -len(kv[0])):
        if phrase in t:
            return root
    return None


def parse_futures_contract_key(
    text: Any,
    *,
    product_root: Any = None,
) -> Optional[str]:
    """
    Normalize futures to fut:ROOT|YYYY-MM.

    AlphaDesk: NQM6 US. Diamond: PricingTicker NQM6 Index or SecurityName NASDAQ 100 E-MINI Jun26.
    """
    parts = _parse_bloomberg_futures_parts(text)
    if parts:
        root, year, month = parts
        return f"fut:{root}|{year:04d}-{month:02d}"
    root = _norm(product_root).upper()
    parsed_root = _parse_bloomberg_futures_parts(root)
    if parsed_root:
        root = parsed_root[0]
    elif not root or len(root) > 6 or " " in root:
        root = _futures_product_root_from_text(text) or ""
    expiry = _parse_futures_expiry_from_description(text)
    if root and expiry:
        year, month = expiry
        return f"fut:{root}|{year:04d}-{month:02d}"
    return None


def is_futures_like_position(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security: Any = None,
    security_name: Any = None,
    cusip: Any = None,
    match_key: Any = None,
) -> bool:
    if _is_futures_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("fut:"):
        return True
    for candidate in (bbg_ticker, security, company_symbol, cusip, security_name, description):
        if parse_futures_contract_key(candidate):
            return True
    return False


def normalize_diamond_futures_close(row: Dict[str, Any]) -> Optional[float]:
    """
    Diamond futures PortfolioPrice is contract notional (index quote × multiplier).

    Use PreDiscountPrice when set (raw index quote), else PortfolioPrice × PriceMultiplier
    (e.g. 153875 × 0.04 = 6155), else PortfolioPrice ÷ QuantityMultiplier
    (e.g. 609765 ÷ 20 = 30488.25 for NQ).
    """
    pre = _nonzero_amount(row.get("PreDiscountPrice"))
    if pre is not None:
        return round(pre, 6)
    portfolio = _nonzero_amount(row.get("PortfolioPrice"))
    if portfolio is None:
        return None
    price_mult = _nonzero_amount(row.get("PriceMultiplier"))
    if price_mult is not None and price_mult > 0:
        return round(portfolio * price_mult, 6)
    qty_mult = _nonzero_amount(row.get("QuantityMultiplier"))
    if qty_mult is not None and qty_mult > 0:
        return round(portfolio / qty_mult, 6)
    return round(portfolio, 6)


def diamond_futures_contract_multiplier(row: Dict[str, Any]) -> float:
    """QuantityMultiplier on Diamond futures rows is the index-point dollar multiplier."""
    qm = _nonzero_amount(row.get("QuantityMultiplier"))
    if qm is not None and qm > 0:
        return qm
    pp = _nonzero_amount(row.get("PortfolioPrice"))
    pre = _nonzero_amount(row.get("PreDiscountPrice"))
    pm = _nonzero_amount(row.get("PriceMultiplier"))
    if pp and pre and pre > 0 and abs(pp / pre - round(pp / pre)) < 0.01:
        inferred = round(pp / pre)
        if inferred > 1:
            return float(inferred)
    if pp and pm and pm > 0:
        inferred = round(1.0 / pm)
        if inferred > 1:
            return float(inferred)
    return 1.0


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
    r"^([A-Z0-9][A-Z0-9.]*)\s+(?:(US)\s+)?(\d{1,2}/\d{1,2}/\d{2,4})\s+([PC])\s*(\d+(?:\.\d+)?)(?:\s+(?:US|EQUITY))*\s*$",
    re.IGNORECASE,
)
_OPTION_LONG_RE = re.compile(
    r"^(CALL|PUT)\s+.+?\$(\d+(?:\.\d+)?)\s+(\d{1,2})([A-Z]{3})(\d{2,4})\s*$",
    re.IGNORECASE,
)
_MONTH_TO_NUM = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
# Bloomberg futures month codes (e.g. NQM6 = NQ Jun 2026).
_FUTURES_MONTH_CODE = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
_FUTURES_BBG_SUFFIX_RE = re.compile(
    r"(?:\s+(?:US|INDEX|COMDTY|PIT|EQUITY))*\s*$",
    re.IGNORECASE,
)
_FUTURES_DESC_EXPIRY_RE = re.compile(
    r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)[A-Z]*\s*(\d{1,2}|\d{4})\s*$",
    re.IGNORECASE,
)
# Diamond SecurityName phrases -> Bloomberg futures root (when only descriptive name is present).
_FUTURES_PRODUCT_ROOT: Dict[str, str] = {
    "NASDAQ 100 E-MINI": "NQ",
    "E-MINI NASDAQ": "NQ",
    "E-MINI S&P": "ES",
    "S&P 500 E-MINI": "ES",
    "E-MINI RUSSELL": "RTY",
    "RUSSELL 2000 E-MINI": "RTY",
}
_PREFERRED_BBG_DOTTED_RE = re.compile(
    r"^([A-Z0-9][A-Z0-9.-]*)\.PR\.([A-Z0-9]+)\.CA$",
    re.IGNORECASE,
)
_PREFERRED_BBG_SPACED_RE = re.compile(
    r"^([A-Z0-9][A-Z0-9.-]*)\s+PR\s+([A-Z0-9]+)(?:\s+CA)?$",
    re.IGNORECASE,
)


def _is_preferred_security_type(security_type: Any) -> bool:
    u = _norm(security_type).upper().replace(" ", "")
    if not u:
        return False
    return u in ("PREFERRED", "PREFERREDSHARE", "PFD", "PREF") or "PREFERRED" in u


def _looks_like_preferred_description(text: Any) -> bool:
    t = normalize_instrument_description(text)
    if not t:
        return False
    if re.search(r"\b(PFD|PREFERRED|PREF(?:ERRED)?\s+SH)\b", t):
        return True
    return bool(re.search(r"\bSERIES\s*[A-Z0-9]+\b", t) and re.search(r"\d+(?:\.\d+)?\s*%", t))


def parse_preferred_bbg_key(text: Any) -> Optional[str]:
    """
    Canonical preferred key from Bloomberg symbology.

    AlphaDesk: BEP.PR.S.CA / PWI.PR.A.CA. Diamond PricingTicker when populated matches directly.
    """
    s = _norm(text).upper()
    if not s:
        return None
    s = re.sub(r"\s+(EQUITY|CORP|PFD|PREFERRED)\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    dotted = s.replace(" ", "")
    m = _PREFERRED_BBG_DOTTED_RE.match(dotted)
    if m:
        root, series = m.group(1).upper(), m.group(2).upper()
        return f"pref:{root}.PR.{series}.CA"
    m = _PREFERRED_BBG_SPACED_RE.match(s)
    if m:
        root, series = m.group(1).upper(), m.group(2).upper()
        return f"pref:{root}.PR.{series}.CA"
    return None


def is_preferred_like_position(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security: Any = None,
    security_name: Any = None,
    match_key: Any = None,
) -> bool:
    if _is_preferred_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("pref:"):
        return True
    for candidate in (security, bbg_ticker, company_symbol):
        if parse_preferred_bbg_key(candidate):
            return True
    for text in (description, security_name, company_symbol):
        if _looks_like_preferred_description(text):
            return True
    return False


def normalize_line_equity_key(text: Any) -> str:
    """
    Compact equity ticker for line: keys — PSNY.US / PSNY US / ABX.CA -> PSNY / ABX.
    Long issuer names (with spaces) pass through unchanged.
    """
    s = normalize_bbg_key(text)
    if not s:
        return ""
    if " " in s and not re.search(r"\s+US$", s, re.IGNORECASE):
        return s
    m = re.match(r"^([A-Z0-9][A-Z0-9.-]*)\.(CA|US|UN|UW|UQ|CN)$", s, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.match(r"^([A-Z0-9][A-Z0-9.-]*)\s+US$", s, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return s


def _line_key_profile(key: str, bucket: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Classify line: keys for orphan pairing — compact ticker, long name, or preferred description."""
    if not key.startswith("line:"):
        return None
    body = key[5:]
    desc = body
    if bucket:
        desc = (
            bucket.get("description")
            or bucket.get("security_name")
            or bucket.get("ticker")
            or body
        )
    if is_preferred_like_position(description=desc, match_key=key):
        return "preferred_desc"
    if " " in body and len(body) > 15:
        return "long"
    if body:
        return "compact"
    return None


def _normalize_strike_key(strike: str) -> str:
    try:
        f = float(strike)
    except ValueError:
        return strike
    if abs(f - round(f)) < 1e-9:
        return str(int(round(f)))
    return f"{f:.6f}".rstrip("0").rstrip(".")


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


def _normalize_expiry_ddmonyyyy(day: str, mon: str, year: str) -> str:
    try:
        d = int(day)
        y = int(year)
        if y < 100:
            y += 2000 if y < 70 else 1900
        m = _MONTH_TO_NUM.get(mon.upper()[:3])
        if not m:
            return f"{year}-{mon}-{day}"
        return f"{y:04d}-{m:02d}-{d:02d}"
    except ValueError:
        return f"{day}{mon}{year}"


def parse_option_contract_key(
    text: Any,
    *,
    underlying_root: Any = None,
) -> Optional[str]:
    """
    Normalize option lines like SPY 06/18/26 P675 US, DRAM US 05/22/26 P51.5 EQUITY,
    and AlphaDesk long names (Put Roundhill Memory ETF $51.50 22MAY2026) to
    opt:ROOT|2026-05-22|P|51.5.
    """
    t = normalize_instrument_description(text)
    if not t:
        return None
    m = _OPTION_COMPACT_RE.match(t)
    if m:
        root, _mkt, expiry, cp, strike = m.groups()
        return (
            f"opt:{root.upper()}|{_normalize_expiry_mdy(expiry)}|{cp.upper()}|"
            f"{_normalize_strike_key(strike)}"
        )
    root = _norm(underlying_root).upper()
    if not root or len(root) > 12 or " " in root:
        return None
    m = _OPTION_LONG_RE.match(t)
    if not m:
        return None
    cp_word, strike, day, mon, year = m.groups()
    cp = "C" if cp_word.upper().startswith("C") else "P"
    return (
        f"opt:{root}|{_normalize_expiry_ddmonyyyy(day, mon, year)}|{cp}|"
        f"{_normalize_strike_key(strike)}"
    )


def build_underlying_ticker_index(records: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map Diamond UnderlyingBBGID / CompositeBBGID to equity root (e.g. HYG) from PricingTicker."""
    index: Dict[str, str] = {}
    for row in records:
        pt = _norm(row.get("PricingTicker"))
        if not pt:
            continue
        root = normalize_bbg_key(pt).split()[0] if normalize_bbg_key(pt) else ""
        if not root or len(root) > 12:
            continue
        for field in ("CompositeBBGID", "UnderlyingBBGID"):
            bbg = _norm(row.get(field))
            if bbg:
                index[bbg] = root
    return index


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
    """PSC SECURITY_TYPE Bond (etc.) is authoritative; patterns are fallback for Diamond."""
    if _is_bond_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("bond:"):
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


def is_option_like_position(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security: Any = None,
    security_name: Any = None,
    match_key: Any = None,
) -> bool:
    """PSC SECURITY_TYPE EquityOption is authoritative; patterns are fallback for Diamond."""
    if _is_option_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("opt:"):
        return True
    for text in (security, bbg_ticker, description, security_name, company_symbol):
        if parse_option_contract_key(text) or _looks_like_option_description(text):
            return True
    return False


def portfolio_details_display_ticker(
    *,
    security_type: Any = None,
    company_symbol: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    security: Any = None,
    security_name: Any = None,
) -> str:
    """
    Display label for reconcile Ticker column (AlphaDesk Security Ticker / ID).

    PSC ph.SECURITY is authoritative for Bond and EquityOption (e.g. TDW 9.125 07-30 US).
    Equities: SECURITY (e.g. ABX.CA) then company_symbol. Matching keys unchanged.
    """
    st = _norm(security_type)
    bbg = _norm(bbg_ticker)
    sec = _norm(security)
    cs = _norm(company_symbol)
    desc = _norm(description)
    sn = _norm(security_name)

    if _is_option_security_type(st):
        return sec or desc or cs or sn or bbg or ""

    if _is_bond_security_type(st):
        return sec or desc or sn or cs or bbg or ""

    if _is_futures_security_type(st):
        return bbg or sec or cs or desc or sn or ""

    st_u = st.upper().replace(" ", "")
    if st_u in ("PREFERRED", "FXFORWARD") or st_u.startswith("FX"):
        return sec or cs or desc or bbg or sn or ""

    return sec or cs or bbg or desc or sn or ""


def align_diamond_bond_close(
    diamond_close: Optional[float],
    alphadesk_close: Optional[float],
    *,
    is_bond_like: bool = False,
    is_option_like: bool = False,
) -> Optional[float]:
    """Scale Diamond fractional par quotes when AlphaDesk is already in par points."""
    if is_option_like or diamond_close is None:
        return diamond_close
    if alphadesk_close is None:
        if is_bond_like and 0 < diamond_close < 20:
            return round(diamond_close * 100.0, 6)
        return diamond_close
    if is_bond_like or (0 < diamond_close < 20 and alphadesk_close > 50):
        if 0 < diamond_close < 20:
            return round(diamond_close * 100.0, 6)
    return diamond_close


def notional_quantity_multiplier(
    security_type: Any,
    description: Any = None,
    *,
    contract_size: Any = None,
) -> float:
    """
    Scale quantity × price into dollars.

    Options: contracts × 100 (shares per contract).
    Bonds: face quantity × (price / 100) — AlphaDesk close is % of par.
    Futures: contracts × index-point multiplier (e.g. NQ = 20).
    Equities / fund units: quantity × price as-is.
    """
    if _is_option_security_type(security_type) or _looks_like_option_description(description):
        return 100.0
    if _is_bond_security_type(security_type):
        return 0.01
    if _is_futures_security_type(security_type) or parse_futures_contract_key(description):
        cs = _nonzero_amount(contract_size)
        return cs if cs is not None else 1.0
    return 1.0


def _nonzero_amount(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if abs(v) < 1e-9:
        return None
    return v


def apply_diamond_price_discount(
    normalized_close: Optional[float],
    *,
    price_discount: Any = None,
    pre_discount_price: Any = None,
    is_bond_like: bool = False,
) -> Optional[float]:
    """
    Sinking-fund bonds: PortfolioPrice is after the sinking discount; add PriceDiscount
    back for a full-par comparable close (e.g. 71.882 + 26 ≈ 97.88 vs AlphaDesk).

    PreDiscountPrice is NOT always full par — it can be the current sinking factor (~27),
    so only use it when it is clearly the restored quote (near normalized + discount).
    """
    discount = _nonzero_amount(price_discount)
    if discount is None:
        return normalized_close
    if normalized_close is None:
        return None
    restored = round(normalized_close + discount, 6)
    if not is_bond_like:
        return restored
    pre = _nonzero_amount(pre_discount_price)
    if pre is None:
        return restored
    # Ignore sinking-factor PreDiscount (~27); accept full-par PreDiscount (~97).
    if pre >= normalized_close + discount * 0.5 and pre >= restored - 1.0:
        return round(pre, 6)
    return restored


def normalize_diamond_close_price(
    price: Any,
    *,
    security_name: Any = None,
    security_type: Any = None,
    description: Any = None,
    bbg_ticker: Any = None,
    is_bond_like: bool = False,
    is_option_like: bool = False,
    price_discount: Any = None,
    pre_discount_price: Any = None,
) -> Optional[float]:
    """
    Diamond reports bond prices as fraction of par (0.99982); AlphaDesk uses 99.982.
    Options and equities are never scaled.

    When PriceDiscount is set (sinking adjustment), add it to the normalized PortfolioPrice.
    """
    if price is None:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if is_option_like or is_option_like_position(
        security_type=security_type,
        bbg_ticker=bbg_ticker,
        description=description,
        security_name=security_name,
    ):
        return round(p, 6)
    bond_like = is_bond_like or is_bond_like_position(
        security_type=security_type,
        company_symbol=security_name,
        description=description,
        security_name=security_name,
    )
    has_sinking_discount = _nonzero_amount(price_discount) is not None
    # Diamond often omits SecurityType on bonds; sinking PriceDiscount implies fractional par.
    if (bond_like or has_sinking_discount) and 0 < p < 20:
        p = round(p * 100.0, 6)
    else:
        p = round(p, 6)
    return apply_diamond_price_discount(
        p,
        price_discount=price_discount,
        pre_discount_price=pre_discount_price,
        is_bond_like=bond_like,
    )


def reconcile_match_key(
    *,
    company_symbol: Any = None,
    bbg_ticker: Any = None,
    sedol: Any = None,
    isin: Any = None,
    cusip: Any = None,
    description: Any = None,
    security: Any = None,
    security_name: Any = None,
    security_type: Any = None,
    underlying_company_symbol: Any = None,
    option_underlying_root: Any = None,
) -> Optional[str]:
    """
    Shared security key for Diamond vs PSC.

    Order: ISIN, options, futures, plausible CUSIP/SEDOL, preferreds, bonds, equity line tickers.
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

    st = _norm(security_type)
    opt_root = (
        _norm(option_underlying_root).upper()
        or _norm(underlying_company_symbol).upper()
        or _norm(company_symbol).upper()
    )
    if opt_root and (" " in opt_root or len(opt_root) > 12):
        opt_root = ""
    for candidate in (
        security,
        company_symbol,
        bbg_ticker,
        security_name,
        description,
    ):
        opt = parse_option_contract_key(candidate)
        if opt:
            return opt
    if opt_root:
        for candidate in (security, description, security_name, company_symbol):
            opt = parse_option_contract_key(candidate, underlying_root=opt_root)
            if opt:
                return opt

    fut_root = _futures_product_root_from_text(security_name or description or company_symbol) or ""
    cs_compact = _norm(company_symbol).upper()
    if cs_compact and " " not in cs_compact:
        parsed_cs = _parse_bloomberg_futures_parts(cs_compact)
        if parsed_cs:
            fut_root = parsed_cs[0]
    for candidate in (
        security,
        bbg_ticker,
        company_symbol,
        cusip,
        security_name,
        description,
    ):
        fut = parse_futures_contract_key(candidate, product_root=fut_root)
        if fut:
            return fut
    if _is_futures_security_type(st):
        expiry = _parse_futures_expiry_from_description(security_name or description or company_symbol)
        if fut_root and expiry:
            year, month = expiry
            return f"fut:{fut_root}|{year:04d}-{month:02d}"

    cus = _norm(cusip).upper()
    if cus and _is_plausible_cusip(cus):
        return f"cusip:{cus}"
    sed = _norm(sedol).upper()
    if sed and _is_plausible_sedol(sed):
        return f"sedol:{sed}"

    for candidate in (security, bbg_ticker, company_symbol, cusip):
        pref = parse_preferred_bbg_key(candidate)
        if pref:
            return pref

    cs = _norm(company_symbol)
    sn = _norm(security_name)
    desc = _norm(description)
    for bond_line in (cs, sn):
        if _is_bond_security_type(st) or _looks_like_bond_description(bond_line):
            if bond_line and _looks_like_bond_description(bond_line):
                return f"bond:{normalize_instrument_description(bond_line)}"
    if (_is_bond_security_type(st) or _looks_like_bond_description(desc)) and _looks_like_bond_description(desc):
        return f"bond:{normalize_instrument_description(desc)}"

    # Equities / ETFs: compact tickers normalize PSNY.US / PSNY US -> PSNY; long names stay as-is.
    for raw in (cs, bbg_ticker, sn, desc):
        compact = normalize_line_equity_key(raw)
        if compact and " " not in compact and len(compact) <= 12:
            return f"line:{compact}"
        long_name = normalize_instrument_description(raw)
        if long_name and " " in long_name and len(long_name) > 15:
            return f"line:{long_name}"

    bbg = normalize_line_equity_key(bbg_ticker)
    if bbg:
        return f"line:{bbg}"
    return None


def pick_display_ticker(
    psc: Optional[Dict[str, Any]],
    dia: Optional[Dict[str, Any]],
) -> str:
    """Portfolio Details card display; prefer AlphaDesk (PSC) fields when present."""
    if psc:
        t = portfolio_details_display_ticker(
            security_type=psc.get("security_type"),
            company_symbol=psc.get("company_symbol"),
            description=psc.get("description"),
            bbg_ticker=psc.get("bbg_ticker"),
            security=psc.get("security"),
        )
        if t:
            return t
    if dia:
        t = portfolio_details_display_ticker(
            security_type=dia.get("security_type"),
            company_symbol=dia.get("company_symbol"),
            description=dia.get("description"),
            bbg_ticker=dia.get("bbg_ticker"),
            security=dia.get("security"),
            security_name=dia.get("security_name"),
        )
        if t:
            return t
    return ""


def _signed_qty(quantity: Any, long_short: Any) -> float:
    q = abs(float(quantity or 0))
    if q <= 0.0001:
        return 0.0
    if _side(long_short) == "short":
        return -q
    return q


def _parse_psc_reconcile_row(row: tuple) -> Dict[str, Any]:
    contract_size = None
    if len(row) > 12 and row[12] is not None:
        try:
            contract_size = float(row[12])
        except (TypeError, ValueError):
            contract_size = None
    underlying_contract_size = None
    if len(row) > 13 and row[13] is not None:
        try:
            underlying_contract_size = float(row[13])
        except (TypeError, ValueError):
            underlying_contract_size = None
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
        "security": _norm(row[10]),
        "underlying_company_symbol": _norm(row[11]) if len(row) > 11 else "",
        "contract_size": contract_size,
        "underlying_contract_size": underlying_contract_size,
        "day_profit": _optional_float(row[14]) if len(row) > 14 else None,
        "prev_posn_date_int": _compact_int_str(row[15]) if len(row) > 15 else None,
        "realized_profit": _optional_float(row[16]) if len(row) > 16 else None,
        "unrealized_profit": _optional_float(row[17]) if len(row) > 17 else None,
        "sec_ccy": _norm(row[18]) if len(row) > 18 else "",
        "portfolio_ccy": _norm(row[19]) if len(row) > 19 else "",
        "fx_settle_to_base": _optional_float(row[20]) if len(row) > 20 else None,
    }


def fetch_psc_positions_for_reconcile(
    cursor: Any,
    portfolio: str,
    posn_date_compact: str,
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT ph.COMPANY_SYMBOL, ph.DESCRIPTION, ph.BBG_TICKER, ph.ISIN, ph.CUSIP, sd.SEDOL, "
        "ph.SECURITY_TYPE, ph.LONG_SHORT, ph.QUANTITY, ph.CLOSE_PRICE, ph.SECURITY, "
        "ph.UNDERLYING_COMPANY_SYMBOL, ph.CONTRACT_SIZE, ph.UNDERLYING_CONTRACT_SIZE, "
        "ph.DAY_PROFIT, ph.PREV_POSN_DATE_INT, ph.REALIZED_PROFIT, ph.UNREALIZED_PROFIT, "
        "ph.SEC_CCY, ph.PORTFOLIO_CCY, ph.FX_SETTLE_TO_BASE "
        "FROM psc_position_history ph "
        "LEFT JOIN psc_security_data sd ON ph.security_sn = sd.security_sn "
        "WHERE ph.PORTFOLIO = ? AND ph.POSN_DATE_INT = ? "
        "AND ("
        "  (ph.QUANTITY IS NOT NULL AND ABS(ph.QUANTITY) > 0.0001) "
        "  OR ABS(COALESCE(ph.REALIZED_PROFIT, 0)) > 0.005 "
        "  OR ABS(COALESCE(ph.UNREALIZED_PROFIT, 0)) > 0.005"
        ")"
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


def _compact_int_str(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        return f"{int(value):08d}"
    except (TypeError, ValueError):
        return None


def _compact_to_iso(compact: Any) -> Optional[str]:
    s = _compact_int_str(compact)
    if not s or len(s) != 8:
        return None
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def _sum_optional_amounts(*values: Any) -> Optional[float]:
    nums = [_optional_float(v) for v in values]
    present = [v for v in nums if v is not None]
    if not present:
        return None
    return round(sum(present), 2)


def _psc_pnl_to_base(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    AlphaDesk realized/unrealized profit fields are local/security currency.

    Convert them to the fund base currency using FX_SETTLE_TO_BASE so the
    comparison is on the same basis as Diamond Base P&L.
    """
    local_pnl = _sum_optional_amounts(
        row.get("realized_profit"),
        row.get("unrealized_profit"),
    )
    if local_pnl is None:
        return None, None, None
    fx = _optional_float(row.get("fx_settle_to_base"))
    if fx is None or fx <= 0:
        fx = 1.0
    return round(local_pnl * fx, 2), local_pnl, fx


def infer_reference_date_from_psc(
    fund_id: str,
    valuation_date_iso: str,
    *,
    dsn: str = "PSC_VIEWER",
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Use AlphaDesk's prior position date as Diamond's ReferenceDate.

    This keeps Diamond's "since reference date" P&L aligned with AlphaDesk's
    realized + unrealized report window, especially across holidays where the
    previous valuation date is not simply the previous weekday.
    """
    candidates = psc_portfolio_candidates_for_fund(fund_id)
    if not candidates:
        return None, None, f"No PSC portfolio mapping for fund {fund_id}"
    try:
        import pyodbc
    except ImportError:
        return None, None, "pyodbc not installed"

    val_compact = normalize_valuation_date(valuation_date_iso).replace("-", "")
    sql_exact = (
        "SELECT MAX(PREV_POSN_DATE_INT) FROM psc_position_history "
        "WHERE PORTFOLIO = ? AND POSN_DATE_INT = ? "
        "AND PREV_POSN_DATE_INT IS NOT NULL"
    )
    sql_like = sql_exact.replace("WHERE PORTFOLIO = ?", "WHERE PORTFOLIO LIKE ?")
    conn = None
    try:
        conn = pyodbc.connect(f"DSN={dsn}")
        cursor = conn.cursor()
        for portfolio in candidates:
            for sql, port in ((sql_exact, portfolio), (sql_like, f"{portfolio}%")):
                cursor.execute(sql, (port, val_compact))
                row = cursor.fetchone()
                ref_iso = _compact_to_iso(row[0] if row else None)
                if ref_iso and ref_iso < normalize_valuation_date(valuation_date_iso):
                    return ref_iso, portfolio, None
        return None, candidates[0], None
    except Exception as exc:
        return None, candidates[0], str(exc)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _psc_match_key(row: Dict[str, Any]) -> Optional[str]:
    return reconcile_match_key(
        company_symbol=row.get("company_symbol"),
        bbg_ticker=row.get("bbg_ticker"),
        sedol=row.get("sedol"),
        isin=row.get("isin"),
        cusip=row.get("cusip"),
        description=row.get("description"),
        security=row.get("security"),
        security_name=row.get("company_symbol"),
        security_type=row.get("security_type"),
        underlying_company_symbol=row.get("underlying_company_symbol"),
    )


def _merge_bucket_into(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    target["shares"] = float(target.get("shares") or 0) + float(source.get("shares") or 0)
    for pnl_field, present_field in (
        ("alphadesk_pnl", "alphadesk_pnl_present"),
        ("diamond_pnl", "diamond_pnl_present"),
    ):
        if source.get(present_field):
            target[pnl_field] = float(target.get(pnl_field) or 0) + float(source.get(pnl_field) or 0)
            target[present_field] = True
    if source.get("close_price") is not None:
        target["close_price"] = source["close_price"]
    for field in ("isin", "cusip", "sedol"):
        if source.get(field) and not target.get(field):
            target[field] = source[field]
    if len(_norm(source.get("ticker"))) > len(_norm(target.get("ticker"))):
        target["ticker"] = source["ticker"]


def _canonical_reconcile_key(*keys: str) -> str:
    """Pick one shared key when PSC and Diamond used different primary keys."""
    order = ("isin:", "cusip:", "sedol:", "opt:", "fut:", "pref:", "bond:", "line:")
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
                if not val:
                    continue
                if field == "cusip" and not _is_plausible_cusip(val):
                    continue
                if field == "sedol" and not _is_plausible_sedol(val):
                    continue
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


def _closes_match_for_orphan_merge(
    psc_close: Optional[float],
    dia_close: Optional[float],
) -> bool:
    if psc_close is None or dia_close is None:
        return False
    pc, dc = float(psc_close), float(dia_close)
    if abs(pc - dc) <= 0.011:
        return True
    denom = max(abs(pc), abs(dc), 1e-9)
    return abs(pc - dc) / denom <= 0.0002


def _orphan_pair_allowed(
    pk: str,
    psc: Dict[str, Any],
    dk: str,
    dia: Dict[str, Any],
) -> bool:
    if psc.get("is_option_like") or dia.get("is_option_like"):
        return False
    if psc.get("is_futures_like") and dia.get("is_futures_like"):
        return True
    if psc.get("is_futures_like") or dia.get("is_futures_like"):
        return False
    if (psc.get("is_bond_like") or dia.get("is_bond_like")) and not (
        is_preferred_like_position(
            security_type=psc.get("security_type"),
            company_symbol=psc.get("company_symbol"),
            description=psc.get("description"),
            bbg_ticker=psc.get("bbg_ticker"),
            match_key=pk,
        )
        or is_preferred_like_position(
            security_type=dia.get("security_type"),
            company_symbol=dia.get("company_symbol"),
            description=dia.get("description") or dia.get("security_name"),
            security_name=dia.get("security_name"),
            bbg_ticker=dia.get("bbg_ticker"),
            match_key=dk,
        )
    ):
        return False

    p_pref = pk.startswith("pref:") or is_preferred_like_position(
        security_type=psc.get("security_type"),
        company_symbol=psc.get("company_symbol"),
        description=psc.get("description") or psc.get("ticker"),
        bbg_ticker=psc.get("bbg_ticker"),
        match_key=pk,
    )
    d_pref = dk.startswith("pref:") or is_preferred_like_position(
        security_type=dia.get("security_type"),
        company_symbol=dia.get("company_symbol"),
        description=dia.get("description") or dia.get("security_name") or dia.get("ticker"),
        security_name=dia.get("security_name") or dia.get("ticker"),
        bbg_ticker=dia.get("bbg_ticker"),
        match_key=dk,
    )
    if p_pref and d_pref:
        return True

    p_prof = _line_key_profile(pk, psc)
    d_prof = _line_key_profile(dk, dia)
    if p_prof == "compact" and d_prof == "long":
        return True
    if p_prof == "long" and d_prof == "compact":
        return True
    return False


def merge_orphan_positions_by_close_and_notional(
    psc_by_key: Dict[str, Dict[str, Any]],
    dia_by_key: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], int]:
    """
    Pair unmatched PSC/Diamond rows when closes and notionals align.

    Handles preferreds (BEP.PR.S.CA vs long PFD description) and equities where
    AlphaDesk has a compact ticker but Diamond only has the issuer name (PSNY.US vs Polestar...).
    """
    psc_only = [k for k in psc_by_key if k not in dia_by_key]
    dia_only = [k for k in dia_by_key if k not in psc_by_key]
    used_dia: set = set()
    merges = 0

    for pk in list(psc_only):
        psc = psc_by_key.get(pk)
        if not psc:
            continue
        pc = psc.get("close_price")
        if pc is None or float(pc) <= 0:
            continue
        best_dk: Optional[str] = None
        best_score = 999.0
        for dk in dia_only:
            if dk in used_dia:
                continue
            dia = dia_by_key.get(dk)
            if not dia:
                continue
            if not _orphan_pair_allowed(pk, psc, dk, dia):
                continue
            dc = dia.get("close_price")
            if not _closes_match_for_orphan_merge(pc, dc):
                continue
            pn = _position_notional(psc)
            dn = _position_notional(dia)
            if pn < 100 or dn < 100:
                continue
            nv_rel = abs(pn - dn) / max(pn, dn)
            if nv_rel > 0.05:
                continue
            if nv_rel < best_score:
                best_score = nv_rel
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
        alphadesk_pnl, alphadesk_pnl_local, fx_settle_to_base = _psc_pnl_to_base(row)
        # Same-day trades can end with zero quantity but still carry realized P&L.
        if abs(signed) <= 0.0001 and (
            alphadesk_pnl is None or abs(alphadesk_pnl) <= 0.005
        ):
            continue
        opt_like = is_option_like_position(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security=row.get("security"),
            match_key=key,
        )
        fut_like = is_futures_like_position(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security=row.get("security"),
            cusip=row.get("cusip"),
            match_key=key,
        )
        pref_like = is_preferred_like_position(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security=row.get("security"),
            match_key=key,
        )
        contract_size = row.get("contract_size") or row.get("underlying_contract_size")
        mult = notional_quantity_multiplier(
            row.get("security_type"),
            row.get("bbg_ticker") or row.get("description") or row.get("security"),
            contract_size=contract_size,
        )
        bucket = out.get(key)
        if not bucket:
            display = portfolio_details_display_ticker(
                security_type=row.get("security_type"),
                company_symbol=row.get("company_symbol"),
                description=row.get("description"),
                bbg_ticker=row.get("bbg_ticker"),
                security=row.get("security"),
            )
            bond_like = is_bond_like_position(
                security_type=row.get("security_type"),
                company_symbol=row.get("company_symbol"),
                description=row.get("description"),
                match_key=key,
            ) and not opt_like and not fut_like and not pref_like
            bucket = {
                "match_key": key,
                "ticker": display,
                "company_symbol": row.get("company_symbol"),
                "description": row.get("description"),
                "bbg_ticker": row.get("bbg_ticker"),
                "security": row.get("security"),
                "shares": 0.0,
                "close_price": row.get("close_price"),
                "alphadesk_pnl": 0.0,
                "alphadesk_pnl_local": 0.0,
                "alphadesk_pnl_present": False,
                "fx_settle_to_base": fx_settle_to_base,
                "sec_ccy": row.get("sec_ccy"),
                "portfolio_ccy": row.get("portfolio_ccy"),
                "prev_posn_date_int": row.get("prev_posn_date_int"),
                "qty_multiplier": mult,
                "security_type": row.get("security_type"),
                "isin": row.get("isin"),
                "cusip": row.get("cusip"),
                "sedol": row.get("sedol"),
                "is_bond_like": bond_like,
                "is_option_like": opt_like,
                "is_futures_like": fut_like,
                "is_preferred_like": pref_like,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if alphadesk_pnl is not None:
            bucket["alphadesk_pnl"] = float(bucket.get("alphadesk_pnl") or 0) + alphadesk_pnl
            bucket["alphadesk_pnl_local"] = (
                float(bucket.get("alphadesk_pnl_local") or 0)
                + float(alphadesk_pnl_local or 0)
            )
            bucket["alphadesk_pnl_present"] = True
        if fx_settle_to_base is not None:
            bucket["fx_settle_to_base"] = fx_settle_to_base
        if row.get("sec_ccy") and not bucket.get("sec_ccy"):
            bucket["sec_ccy"] = row.get("sec_ccy")
        if row.get("portfolio_ccy") and not bucket.get("portfolio_ccy"):
            bucket["portfolio_ccy"] = row.get("portfolio_ccy")
        if row.get("close_price") is not None:
            bucket["close_price"] = row.get("close_price")
        if row.get("prev_posn_date_int") and not bucket.get("prev_posn_date_int"):
            bucket["prev_posn_date_int"] = row.get("prev_posn_date_int")
        bucket["ticker"] = portfolio_details_display_ticker(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=bucket.get("description") or row.get("description"),
            bbg_ticker=row.get("bbg_ticker") or bucket.get("bbg_ticker"),
            security=row.get("security") or bucket.get("security"),
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


def _diamond_match_key(
    row: Dict[str, Any],
    *,
    underlying_index: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    sec_name = row.get("SecurityName")
    sec_type = row.get("SecurityType") or row.get("AssetType")
    if is_cash_position(
        security_name=sec_name,
        bbg_ticker=row.get("PricingTicker"),
        security_type=sec_type,
    ):
        return None
    opt_root = ""
    if underlying_index:
        for field in ("UnderlyingBBGID", "CompositeBBGID"):
            bbg = _norm(row.get(field))
            if bbg and bbg in underlying_index:
                opt_root = underlying_index[bbg]
                break
    cs = _norm(row.get("CompanySymbol"))
    if not opt_root and cs and len(cs) <= 12 and " " not in cs:
        opt_root = cs.upper()
    return reconcile_match_key(
        company_symbol=cs or sec_name,
        bbg_ticker=row.get("PricingTicker"),
        sedol=row.get("SEDOL"),
        isin=row.get("ISIN"),
        cusip=row.get("CUSIP"),
        description=sec_name,
        security_name=sec_name,
        security_type=sec_type,
        option_underlying_root=opt_root,
    )


def aggregate_diamond_by_security(
    records: List[Dict[str, Any]],
    valuation_date_iso: str,
) -> Dict[str, Dict[str, Any]]:
    """Roll Diamond holdings to one row per reconcile_match_key."""
    out: Dict[str, Dict[str, Any]] = {}
    dated = [r for r in records if _normalize_quote_date(r.get("QuoteDate"), valuation_date_iso)]
    use_rows = dated if dated else records
    underlying_index = build_underlying_ticker_index(records)
    for row in use_rows:
        sec_name = row.get("SecurityName")
        sec_type = row.get("SecurityType") or row.get("AssetType")
        if is_cash_position(
            security_name=sec_name,
            bbg_ticker=row.get("PricingTicker"),
            security_type=sec_type,
        ):
            continue
        key = _diamond_match_key(row, underlying_index=underlying_index)
        if not key:
            continue
        signed = _diamond_signed_qty(row)
        if abs(signed) <= 0.0001:
            continue
        pricing = row.get("PricingTicker")
        opt_like = is_option_like_position(
            security_type=sec_type,
            company_symbol=sec_name,
            security_name=sec_name,
            bbg_ticker=pricing,
            match_key=key,
        )
        fut_like = is_futures_like_position(
            security_type=sec_type,
            company_symbol=sec_name,
            security_name=sec_name,
            bbg_ticker=pricing,
            cusip=row.get("CUSIP"),
            match_key=key,
        )
        pref_like = is_preferred_like_position(
            security_type=sec_type,
            company_symbol=sec_name,
            security_name=sec_name,
            bbg_ticker=pricing,
            match_key=key,
        )
        bond_like = (
            is_bond_like_position(
                security_type=sec_type,
                company_symbol=sec_name,
                security_name=sec_name,
                match_key=key,
            )
            or _nonzero_amount(row.get("PriceDiscount")) is not None
        ) and not opt_like and not fut_like and not pref_like
        price_disc = row.get("PriceDiscount")
        if fut_like:
            close_f = normalize_diamond_futures_close(row)
        else:
            close_f = normalize_diamond_close_price(
                row.get("PortfolioPrice"),
                security_name=sec_name,
                security_type=sec_type,
                bbg_ticker=pricing,
                is_bond_like=bond_like,
                is_option_like=opt_like,
                price_discount=price_disc,
                pre_discount_price=row.get("PreDiscountPrice"),
            )
        mult = (
            diamond_futures_contract_multiplier(row)
            if fut_like
            else notional_quantity_multiplier(sec_type, pricing or sec_name)
        )
        diamond_pnl = _sum_optional_amounts(
            row.get("BaseUnrealizedGainLossSinceReferenceDate"),
            row.get("BaseRealizedGainLossSinceReferenceDate"),
        )
        bucket = out.get(key)
        if not bucket:
            bucket = {
                "match_key": key,
                "ticker": portfolio_details_display_ticker(
                    security_type=sec_type,
                    company_symbol=sec_name,
                    bbg_ticker=pricing,
                    security_name=sec_name,
                    description=sec_name,
                ),
                "company_symbol": sec_name,
                "security_name": sec_name,
                "description": sec_name,
                "bbg_ticker": pricing,
                "shares": 0.0,
                "close_price": close_f,
                "diamond_pnl": 0.0,
                "diamond_pnl_present": False,
                "qty_multiplier": mult,
                "security_type": sec_type,
                "isin": row.get("ISIN"),
                "cusip": row.get("CUSIP"),
                "sedol": row.get("SEDOL"),
                "is_bond_like": bond_like,
                "is_option_like": opt_like,
                "is_futures_like": fut_like,
                "is_preferred_like": pref_like,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if diamond_pnl is not None:
            bucket["diamond_pnl"] = float(bucket.get("diamond_pnl") or 0) + diamond_pnl
            bucket["diamond_pnl_present"] = True
        if close_f is not None:
            bucket["close_price"] = close_f
        if fut_like:
            bucket["qty_multiplier"] = diamond_futures_contract_multiplier(row)
        bucket["bbg_ticker"] = pricing or bucket.get("bbg_ticker")
        if sec_name and _looks_like_bond_description(sec_name):
            bucket["description"] = sec_name
        bucket["ticker"] = portfolio_details_display_ticker(
            security_type=sec_type,
            company_symbol=sec_name,
            bbg_ticker=bucket.get("bbg_ticker"),
            security_name=sec_name,
            description=bucket.get("description"),
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
    psc_by_key, diamond_by_key, orphan_merges = merge_orphan_positions_by_close_and_notional(
        psc_by_key, diamond_by_key
    )
    meta["orphan_close_merges"] = orphan_merges

    all_keys = sorted(set(psc_by_key.keys()) | set(diamond_by_key.keys()))
    lines: List[Dict[str, Any]] = []
    for key in all_keys:
        psc = psc_by_key.get(key)
        dia = diamond_by_key.get(key)
        psc_close = psc.get("close_price") if psc else None
        dia_close_raw = dia.get("close_price") if dia else None
        option_like = bool(
            (psc and psc.get("is_option_like"))
            or (dia and dia.get("is_option_like"))
            or (key or "").startswith("opt:")
        )
        futures_like = bool(
            (psc and psc.get("is_futures_like"))
            or (dia and dia.get("is_futures_like"))
            or (key or "").startswith("fut:")
        )
        bond_like = (
            not option_like
            and not futures_like
            and bool(
                (psc and psc.get("is_bond_like"))
                or (dia and dia.get("is_bond_like"))
                or is_bond_like_position(match_key=key)
            )
        )
        dia_close = align_diamond_bond_close(
            dia_close_raw,
            float(psc_close) if psc_close is not None else None,
            is_bond_like=bond_like,
            is_option_like=option_like,
        )
        if dia and dia_close is not None:
            dia = {**dia, "close_price": dia_close}
        price_diff, dollar_diff, shares = _compute_dollar_difference(psc, dia)
        diamond_pnl = (
            round(float(dia.get("diamond_pnl") or 0), 2)
            if dia and dia.get("diamond_pnl_present")
            else None
        )
        alphadesk_pnl = (
            round(float(psc.get("alphadesk_pnl") or 0), 2)
            if psc and psc.get("alphadesk_pnl_present")
            else None
        )
        alphadesk_pnl_local = (
            round(float(psc.get("alphadesk_pnl_local") or 0), 2)
            if psc and psc.get("alphadesk_pnl_present")
            else None
        )
        pnl_difference = (
            round(diamond_pnl - alphadesk_pnl, 2)
            if diamond_pnl is not None and alphadesk_pnl is not None
            else None
        )
        ticker = pick_display_ticker(psc, dia)
        lines.append(
            {
                "match_key": key,
                "ticker": ticker or key,
                "diamond_pnl": diamond_pnl,
                "alphadesk_pnl": alphadesk_pnl,
                "alphadesk_pnl_local": alphadesk_pnl_local,
                "alphadesk_sec_ccy": psc.get("sec_ccy") if psc else None,
                "alphadesk_portfolio_ccy": psc.get("portfolio_ccy") if psc else None,
                "alphadesk_fx_settle_to_base": psc.get("fx_settle_to_base") if psc else None,
                "pnl_difference": pnl_difference,
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
    meta["lines_with_pnl_diff"] = sum(
        1
        for r in lines
        if r.get("pnl_difference") is not None and abs(float(r["pnl_difference"])) > 0.01
    )
    meta["total_pnl_difference"] = round(
        sum(float(r["pnl_difference"] or 0) for r in lines if r.get("pnl_difference") is not None),
        2,
    )
    return lines, meta


def fetch_close_price_reconciliation(
    fund_id: str,
    valuation_date_iso: str,
    diamond_client: Any,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """Load Diamond portfolio and PSC, return reconciliation lines."""
    valuation_date_norm = normalize_valuation_date(valuation_date_iso)
    reference_date, reference_portfolio, reference_error = infer_reference_date_from_psc(
        fund_id,
        valuation_date_norm,
    )
    reference_source = "alphadesk_prev_posn_date"
    if not reference_date:
        reference_date = prior_business_day_iso(valuation_date_norm)
        reference_source = "prior_business_day_fallback"
    try:
        raw = diamond_client.get_portfolio(
            fund_id=fund_id,
            valuation_date=valuation_date_norm,
            reference_date=reference_date,
        )
    except Exception as exc:
        return [], {"fund_id": fund_id, "valuation_date": valuation_date_norm}, str(exc)
    lines, meta = build_close_price_reconciliation(fund_id, valuation_date_norm, raw)
    meta["fund_id"] = fund_id
    meta["valuation_date"] = valuation_date_norm
    meta["diamond_reference_date"] = reference_date
    meta["diamond_reference_date_source"] = reference_source
    if reference_portfolio:
        meta["diamond_reference_psc_portfolio"] = reference_portfolio
    if reference_error:
        meta["diamond_reference_warning"] = reference_error
    return lines, meta, None
