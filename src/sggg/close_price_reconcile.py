"""Line-by-line closing price reconciliation: Diamond GetPortfolio vs AlphaDesk PSC."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sggg.compliance_check_estimates import FUND_NAME_TO_ID
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
    # Bloomberg corp/FIGI tickers (e.g. US923725AE50@bvn4) must not match month codes.
    if "@" in token:
        return None
    # Non-greedy root: NQM6 -> NQ + M + 6, not NQM + M + 6.
    m = re.match(r"^(.+?)([FGHJKMNQUVXZ])(\d{1,2})$", token)
    if not m:
        return None
    root, month_code, year_digits = m.groups()
    month = _FUTURES_MONTH_CODE.get(month_code.upper())
    if not month or not root or len(root) > 6:
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


_FUTURES_OPTION_BBG_RE = re.compile(
    r"^([A-Z0-9]+[FGHJKMNQUVXZ]\d[CP])\s+(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_FUTURES_OPTION_ALPHADESK_RE = re.compile(
    r"^([A-Z0-9]+[FGHJKMNQUVXZ]\d[CP])\s+"
    r"(\d{1,2}/\d{1,2}/\d{2,4})\s+([CP])\s*(\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)
_FUTURES_OPTION_DIAMOND_NAME_RE = re.compile(
    r"(?:CRUDE\s+OIL|WTI)(?:\s+(?:FUT\s+OPT|OPT\s+IPE))?\s+"
    r"([A-Z]{3})(\d{2})([CP])\s+(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _futopt_key_from_bbg_parts(
    root: str,
    month_code: str,
    year_digit: str,
    cp: str,
    strike: str,
) -> Optional[str]:
    month = _FUTURES_MONTH_CODE.get(month_code.upper())
    if not month:
        return None
    year = _futures_year_from_digits(year_digit)
    return (
        f"futopt:{root.upper()}|{year:04d}-{month:02d}|"
        f"{cp.upper()}|{_normalize_strike_key(strike)}"
    )


def _split_futures_option_bbg_ticker(token: Any) -> Optional[Tuple[str, str]]:
    """
    Bloomberg futures option tickers append C/P after the futures month code.

    CLQ6C / COU6P -> (CLQ6, C), (COU6, P).
    """
    raw = _norm(token).upper().split()[0]
    m = re.match(r"^(.+[FGHJKMNQUVXZ]\d)([CP])$", raw)
    if not m:
        return None
    return m.group(1), m.group(2)


def _futopt_key_from_option_ticker_and_strike(
    option_ticker: str,
    strike: str,
    *,
    cp: Optional[str] = None,
) -> Optional[str]:
    underlying, series_cp = _split_futures_option_bbg_ticker(option_ticker) or (None, None)
    if not underlying:
        return None
    cp_use = (cp or series_cp or "").upper()
    if cp_use not in ("C", "P"):
        return None
    parts = _parse_bloomberg_futures_parts(underlying)
    if not parts:
        return None
    _root, year, month = parts
    return (
        f"futopt:{underlying.upper()}|{year:04d}-{month:02d}|{cp_use}|"
        f"{_normalize_strike_key(strike)}"
    )


def parse_futures_option_contract_key(text: Any) -> Optional[str]:
    """
    Normalize futures options across AlphaDesk and Diamond naming.

    AlphaDesk: CLQ6C 08/16/26 C77.5
    Diamond BBG: CLQ6C 77.5 PIT / PricingTicker COU6C 82
    Diamond name: CRUDE OIL FUT OPT Aug26C 77.5 / CRUDE OIL OPT IPE Sep26C 82

    Canonical: futopt:CL|2026-08|C|77.5 (underlying futures month + call/put + strike).
    """
    t = normalize_instrument_description(text)
    if not t:
        return None

    m = _FUTURES_OPTION_ALPHADESK_RE.match(t)
    if m:
        option_ticker, _expiry, cp, strike = m.groups()
        return _futopt_key_from_option_ticker_and_strike(
            option_ticker,
            strike,
            cp=cp,
        )

    m = _FUTURES_OPTION_BBG_RE.match(t)
    if m:
        option_ticker, strike = m.groups()
        return _futopt_key_from_option_ticker_and_strike(option_ticker, strike)

    m = _FUTURES_OPTION_DIAMOND_NAME_RE.search(t)
    if m:
        mon, year2, cp, strike = m.groups()
        month = _MONTH_TO_NUM.get(mon.upper()[:3])
        if not month:
            return None
        year = _futures_year_from_digits(year2)
        if "OPT IPE" in t.upper():
            product_root = "CO"
        else:
            product_root = _futures_product_root_from_text(t) or "CL"
        month_code = next(
            (code for code, num in _FUTURES_MONTH_CODE.items() if num == month),
            None,
        )
        year_digit = year2[-1] if year2 else str(year % 10)
        underlying = (
            f"{product_root}{month_code}{year_digit}" if month_code else product_root
        )
        return (
            f"futopt:{underlying.upper()}|{year:04d}-{month:02d}|"
            f"{cp.upper()}|{_normalize_strike_key(strike)}"
        )
    return None


def is_futures_option_like_position(
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
    mk = _norm(match_key)
    if mk.startswith("futopt:"):
        return True
    for candidate in (
        bbg_ticker,
        security,
        company_symbol,
        cusip,
        security_name,
        description,
    ):
        if parse_futures_option_contract_key(candidate):
            return True
    return False


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
    if _is_bond_security_type(security_type):
        return False
    if _is_futures_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("fut:"):
        return True
    for candidate in (bbg_ticker, security, company_symbol, security_name, description):
        if _looks_like_bond_description(candidate):
            continue
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
    if _OPTION_LONG_RE.match(t):
        return True
    return bool(
        re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t)
        and re.search(r"\b([PC])\s*\d", t)
    )


def _is_diamond_option_trade(trade: Dict[str, Any]) -> bool:
    if is_futures_option_like_position(
        security_type=trade.get("SecurityType") or trade.get("AssetType"),
        company_symbol=trade.get("Ticker"),
        description=trade.get("SecurityName"),
        bbg_ticker=trade.get("Ticker"),
        security=trade.get("SecurityName"),
        security_name=trade.get("SecurityName"),
    ):
        return False
    sec_name = trade.get("SecurityName")
    sec_type = trade.get("SecurityType") or trade.get("AssetType")
    ticker = trade.get("Ticker")
    if _is_option_security_type(sec_type):
        return True
    if _looks_like_option_description(sec_name) or _looks_like_option_description(ticker):
        return True
    t = _norm(ticker).upper()
    if re.match(r"^[A-Z0-9.]+\s+\d+\s+[PC]\d", t):
        return True
    name = _norm(sec_name).upper()
    return name.startswith("PUT ") or name.startswith("CALL ")


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
    "CRUDE OIL": "CL",
    "WTI CRUDE": "CL",
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
    bbg_ticker: Any = None,
    match_key: Any = None,
) -> bool:
    """PSC SECURITY_TYPE Bond (etc.) is authoritative; patterns are fallback for Diamond."""
    if is_futures_option_like_position(
        security_type=security_type,
        company_symbol=company_symbol,
        description=description,
        security_name=security_name,
        bbg_ticker=bbg_ticker,
        match_key=match_key,
    ):
        return False
    if _is_bond_security_type(security_type):
        return True
    mk = _norm(match_key)
    if mk.startswith("bond:"):
        return True
    for text in (company_symbol, description, security_name, bbg_ticker):
        if _looks_like_bond_description(text):
            return True
    for text in (security_name, description):
        t = _norm(text).upper()
        if t and re.search(r"\d+\.?\d*\s*%", t):
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
    if mk.startswith("futopt:"):
        return False
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
    if _is_bond_security_type(security_type) or _looks_like_bond_description(description):
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

    for candidate in (
        bbg_ticker,
        security,
        company_symbol,
        security_name,
        description,
        cusip,
    ):
        futopt = parse_futures_option_contract_key(candidate)
        if futopt:
            return futopt
    bbg = _norm(bbg_ticker)
    cus = _norm(cusip).upper()
    if cus and bbg:
        futopt = parse_futures_option_contract_key(f"{cus} {bbg}")
        if futopt:
            return futopt

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


def _collect_mtm_lookup_keys(
    reconcile_key: str,
    psc: Optional[Dict[str, Any]],
    dia: Optional[Dict[str, Any]],
) -> List[str]:
    """
    Resolve alternate reconcile keys for MTM snapshot/trade lookup.

    After merge_positions_by_secondary_ids the loop uses a canonical key (often ISIN
    from AlphaDesk) while raw Diamond positions and trades may be indexed under
    CUSIP or line:ticker keys.
    """
    keys: List[str] = []

    def add(candidate: Optional[str]) -> None:
        if candidate and candidate not in keys:
            keys.append(candidate)

    add(reconcile_key)
    for row in (psc, dia):
        if not row:
            continue
        add(
            reconcile_match_key(
                company_symbol=row.get("company_symbol"),
                bbg_ticker=row.get("bbg_ticker") or row.get("pricing_ticker"),
                sedol=row.get("sedol"),
                isin=row.get("isin"),
                cusip=row.get("cusip"),
                description=row.get("description") or row.get("security_name"),
                security=row.get("security"),
                security_name=row.get("security_name"),
                security_type=row.get("security_type"),
            )
        )
        for raw in (
            row.get("security"),
            row.get("company_symbol"),
            row.get("bbg_ticker"),
            row.get("pricing_ticker"),
        ):
            compact = normalize_line_equity_key(raw)
            if compact:
                add(f"line:{compact}")
    return keys


def _lookup_first(
    index: Dict[str, Any],
    lookup_keys: List[str],
) -> Optional[Any]:
    for candidate in lookup_keys:
        if candidate in index:
            return index[candidate]
    return None


def _lookup_trades_for_keys(
    trade_index: Dict[str, List[Dict[str, Any]]],
    lookup_keys: List[str],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set = set()
    for candidate in lookup_keys:
        for trade in trade_index.get(candidate, []):
            trade_id = trade.get("TradeID")
            if not trade_id:
                trade_id = (
                    trade.get("TradeDate"),
                    trade.get("qty") if trade.get("qty") is not None else trade.get("Quantity"),
                    trade.get("price")
                    if trade.get("price") is not None
                    else trade.get("LocalAmountPerShare"),
                )
            if trade_id in seen:
                continue
            seen.add(trade_id)
            merged.append(trade)
    return merged


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
        "sec_ccy": _norm(row[16]) if len(row) > 16 else "",
        "portfolio_ccy": _norm(row[17]) if len(row) > 17 else "",
        "fx_settle_to_base": _optional_float(row[18]) if len(row) > 18 else None,
        "dividends": _optional_float(row[19]) if len(row) > 19 else None,
        "posn_date_int": _compact_int_str(row[20]) if len(row) > 20 else None,
        "dividend_ex_date_int": _compact_int_str(row[21]) if len(row) > 21 else None,
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
        "ph.DAY_PROFIT, ph.PREV_POSN_DATE_INT, "
        "ph.SEC_CCY, ph.PORTFOLIO_CCY, ph.FX_SETTLE_TO_BASE, ph.DIVIDENDS, "
        "ph.POSN_DATE_INT, sd.DIVIDEND_EX_DATE_INT "
        "FROM psc_position_history ph "
        "LEFT JOIN psc_security_data sd ON ph.security_sn = sd.security_sn "
        "WHERE ph.PORTFOLIO = ? AND ph.POSN_DATE_INT = ? "
        "AND ("
        "  (ph.QUANTITY IS NOT NULL AND ABS(ph.QUANTITY) > 0.0001) "
        "  OR ABS(COALESCE(ph.DAY_PROFIT, 0)) > 0.005 "
        "  OR (sd.DIVIDEND_EX_DATE_INT = ph.POSN_DATE_INT AND ABS(COALESCE(ph.DIVIDENDS, 0)) > 0.005)"
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


def _psc_same_ex_date_dividends(row: Dict[str, Any]) -> Optional[float]:
    dividends = _optional_float(row.get("dividends"))
    if dividends is None:
        return None
    posn_date = _compact_int_str(row.get("posn_date_int"))
    ex_date = _compact_int_str(row.get("dividend_ex_date_int"))
    if not posn_date or ex_date != posn_date:
        return None
    return dividends


def _sum_optional_amounts(*values: Any) -> Optional[float]:
    nums = [_optional_float(v) for v in values]
    present = [v for v in nums if v is not None]
    if not present:
        return None
    return round(sum(present), 2)


def _psc_pnl_to_base(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    AlphaDesk DAY_PROFIT is the daily report P&L line item.

    PSC stamps this in portfolio/base currency already; keep FX metadata only
    for diagnostics.
    """
    local_pnl = _optional_float(row.get("day_profit"))
    if local_pnl is None:
        return None, None, None
    fx = _optional_float(row.get("fx_settle_to_base"))
    if fx is None or fx <= 0:
        fx = 1.0
    return round(local_pnl, 2), local_pnl, fx


def infer_reference_date_from_psc(
    fund_id: str,
    valuation_date_iso: str,
    *,
    dsn: str = "PSC_VIEWER",
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Use AlphaDesk's prior position date as Diamond's ReferenceDate.

    This keeps Diamond's "since reference date" P&L aligned with AlphaDesk's
    daily P&L window, especially across holidays where the previous valuation
    date is not simply the previous weekday.
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
    order = ("futopt:", "isin:", "cusip:", "sedol:", "opt:", "fut:", "pref:", "bond:", "line:")
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
        alphadesk_dividends = _psc_same_ex_date_dividends(row)
        # Same-day trades can end with zero quantity but still carry daily P&L/dividends.
        if abs(signed) <= 0.0001 and (
            alphadesk_pnl is None or abs(alphadesk_pnl) <= 0.005
        ) and (
            alphadesk_dividends is None or abs(alphadesk_dividends) <= 0.005
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
        futopt_like = is_futures_option_like_position(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security=row.get("security"),
            cusip=row.get("cusip"),
            match_key=key,
        )
        if futopt_like:
            fut_like = False
            opt_like = False
        pref_like = is_preferred_like_position(
            security_type=row.get("security_type"),
            company_symbol=row.get("company_symbol"),
            description=row.get("description"),
            bbg_ticker=row.get("bbg_ticker"),
            security=row.get("security"),
            match_key=key,
        )
        contract_size = row.get("contract_size") or row.get("underlying_contract_size")
        if futopt_like:
            cs = _nonzero_amount(contract_size)
            mult = cs if cs is not None else 1000.0
        else:
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
                description=row.get("description") or row.get("security"),
                bbg_ticker=row.get("bbg_ticker"),
                match_key=key,
            ) and not opt_like and not fut_like and not pref_like and not futopt_like
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
                "alphadesk_dividends": 0.0,
                "alphadesk_dividends_present": False,
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
                "is_futures_option_like": futopt_like,
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
        if alphadesk_dividends is not None:
            bucket["alphadesk_dividends"] = (
                float(bucket.get("alphadesk_dividends") or 0) + alphadesk_dividends
            )
            bucket["alphadesk_dividends_present"] = True
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


def _quote_date_iso(val: Any) -> Optional[str]:
    quote = _norm(val)
    if not quote:
        return None
    if len(quote) >= 10:
        return normalize_valuation_date(quote[:10])
    compact = quote.replace("-", "")[:8]
    if len(compact) == 8:
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    return None


def _diamond_rows_for_valuation_date(
    records: List[Dict[str, Any]],
    valuation_date_iso: str,
) -> List[Dict[str, Any]]:
    """
    Pick the best Diamond row per security for a valuation date.

    Do not drop names whose QuoteDate lags the portfolio (e.g. Jul 2 quote on a
    Jul 3 reference after a holiday) just because other holdings have Jul 3 quotes.
    """
    if not records:
        return []
    val = normalize_valuation_date(valuation_date_iso)
    underlying_index = build_underlying_ticker_index(records)
    legs: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in records:
        key = _diamond_match_key(row, underlying_index=underlying_index)
        if not key:
            continue
        legs[key].append(row)

    selected: List[Dict[str, Any]] = []
    for rows in legs.values():
        exact = [r for r in rows if _normalize_quote_date(r.get("QuoteDate"), val)]
        if exact:
            selected.extend(exact)
            continue
        on_or_before: List[tuple[str, Dict[str, Any]]] = []
        for row in rows:
            qiso = _quote_date_iso(row.get("QuoteDate"))
            if qiso and qiso <= val:
                on_or_before.append((qiso, row))
        if on_or_before:
            best = max(q for q, _ in on_or_before)
            selected.extend(row for q, row in on_or_before if q == best)
        else:
            selected.extend(rows)
    return selected


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
    use_rows = _diamond_rows_for_valuation_date(records, valuation_date_iso)
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
        diamond_total_unrealized = _optional_float(row.get("BaseTotalUnrealizedGainLoss"))
        diamond_unrealized_since_ref = _optional_float(
            row.get("BaseUnrealizedGainLossSinceReferenceDate")
        )
        diamond_realized_since_ref = _optional_float(
            row.get("BaseRealizedGainLossSinceReferenceDate")
        )
        diamond_dividends = _sum_optional_amounts(
            row.get("BaseDividendIncomeSinceReferenceDate"),
            row.get("BaseWithholdingTaxSinceReferenceDate"),
        )
        has_material_pnl = any(
            abs(v) > 0.005
            for v in (
                diamond_total_unrealized,
                diamond_unrealized_since_ref,
                diamond_realized_since_ref,
                diamond_dividends,
            )
            if v is not None
        )
        if abs(signed) <= 0.0001 and not has_material_pnl:
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
        futopt_like = is_futures_option_like_position(
            security_type=sec_type,
            company_symbol=sec_name,
            security_name=sec_name,
            bbg_ticker=pricing,
            cusip=row.get("CUSIP"),
            match_key=key,
        )
        if futopt_like:
            fut_like = False
            opt_like = False
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
                description=pricing,
                bbg_ticker=pricing,
                match_key=key,
            )
            or _nonzero_amount(row.get("PriceDiscount")) is not None
        ) and not opt_like and not fut_like and not pref_like and not futopt_like
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
        if futopt_like:
            qm = _nonzero_amount(row.get("QuantityMultiplier"))
            mult = qm if qm is not None else 1000.0
        elif fut_like:
            mult = diamond_futures_contract_multiplier(row)
        elif bond_like:
            mult = 0.01
        else:
            mult = notional_quantity_multiplier(sec_type, pricing or sec_name)
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
                "diamond_total_unrealized": 0.0,
                "diamond_total_unrealized_present": False,
                "diamond_unrealized_since_ref": 0.0,
                "diamond_unrealized_since_ref_present": False,
                "diamond_realized_since_ref": 0.0,
                "diamond_realized_since_ref_present": False,
                "diamond_dividends": 0.0,
                "diamond_dividends_present": False,
                "qty_multiplier": mult,
                "security_type": sec_type,
                "isin": row.get("ISIN"),
                "cusip": row.get("CUSIP"),
                "sedol": row.get("SEDOL"),
                "is_bond_like": bond_like,
                "is_option_like": opt_like,
                "is_futures_like": fut_like,
                "is_futures_option_like": futopt_like,
                "is_preferred_like": pref_like,
            }
            out[key] = bucket
        bucket["shares"] = float(bucket["shares"]) + signed
        if diamond_total_unrealized is not None:
            bucket["diamond_total_unrealized"] = (
                float(bucket.get("diamond_total_unrealized") or 0)
                + diamond_total_unrealized
            )
            bucket["diamond_total_unrealized_present"] = True
        if diamond_unrealized_since_ref is not None:
            bucket["diamond_unrealized_since_ref"] = (
                float(bucket.get("diamond_unrealized_since_ref") or 0)
                + diamond_unrealized_since_ref
            )
            bucket["diamond_unrealized_since_ref_present"] = True
        if diamond_realized_since_ref is not None:
            bucket["diamond_realized_since_ref"] = (
                float(bucket.get("diamond_realized_since_ref") or 0)
                + diamond_realized_since_ref
            )
            bucket["diamond_realized_since_ref_present"] = True
        if diamond_dividends is not None:
            bucket["diamond_dividends"] = (
                float(bucket.get("diamond_dividends") or 0) + diamond_dividends
            )
            bucket["diamond_dividends_present"] = True
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
    prior_diamond_raw: Any = None,
    reference_date_iso: Optional[str] = None,
    trades_raw: Any = None,
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
    prior_diamond_records: List[Dict[str, Any]] = []
    prior_diamond_by_key: Dict[str, Dict[str, Any]] = {}
    pos_t_by_key: Dict[str, Dict[str, Any]] = {}
    pos_prior_by_key: Dict[str, Dict[str, Any]] = {}
    trade_index: Dict[str, List[Dict[str, Any]]] = {}
    trade_qty_mismatches: List[Dict[str, Any]] = []
    if prior_diamond_raw is not None and reference_date_iso:
        prior_diamond_records = flatten_diamond_portfolio_records(prior_diamond_raw)
        meta["diamond_prior_positions"] = len(prior_diamond_records)
        prior_diamond_by_key = aggregate_diamond_by_security(
            prior_diamond_records,
            reference_date_iso,
        )
        pos_t_by_key = _index_diamond_positions_by_match_key(diamond_records, valuation_date_iso)
        pos_prior_by_key = _index_diamond_positions_by_match_key(
            prior_diamond_records,
            reference_date_iso,
        )
    if trades_raw is not None and reference_date_iso:
        underlying_index = build_underlying_ticker_index(diamond_records)
        trade_index = build_trade_index_by_match_key(
            flatten_diamond_trade_records(trades_raw),
            valuation_date_iso,
            underlying_index=underlying_index,
        )
        meta["diamond_trades_on_T"] = sum(len(v) for v in trade_index.values())
    meta["diamond_pnl_method"] = "daily_mtm"
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
        futures_option_like = bool(
            (psc and psc.get("is_futures_option_like"))
            or (dia and dia.get("is_futures_option_like"))
            or (key or "").startswith("futopt:")
        )
        if futures_option_like:
            option_like = False
        bond_like = bool(
            not option_like
            and not futures_option_like
            and bool(
                (psc and psc.get("is_bond_like"))
                or (dia and dia.get("is_bond_like"))
                or is_bond_like_position(
                    match_key=key,
                    security_type=(psc or dia or {}).get("security_type"),
                    company_symbol=(psc or {}).get("company_symbol"),
                    description=(psc or {}).get("description")
                    or (psc or {}).get("security")
                    or (dia or {}).get("description"),
                    security_name=(dia or {}).get("security_name"),
                    bbg_ticker=(psc or {}).get("bbg_ticker") or (dia or {}).get("bbg_ticker"),
                )
            )
        )
        futures_like = bool(
            not bond_like
            and not futures_option_like
            and (
                (psc and psc.get("is_futures_like"))
                or (dia and dia.get("is_futures_like"))
                or ((key or "").startswith("fut:"))
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
        prior_dia = prior_diamond_by_key.get(key)
        mtm_lookup_keys = _collect_mtm_lookup_keys(key, psc, dia)
        pos_T = _lookup_first(pos_t_by_key, mtm_lookup_keys)
        pos_prior = _lookup_first(pos_prior_by_key, mtm_lookup_keys)
        trades_for_symbol = _lookup_trades_for_keys(trade_index, mtm_lookup_keys)
        diamond_pnl: Optional[float] = None
        diamond_total_unrealized = (
            float(dia.get("diamond_total_unrealized") or 0)
            if dia and dia.get("diamond_total_unrealized_present")
            else None
        )
        diamond_prior_total_unrealized = (
            float(prior_dia.get("diamond_total_unrealized") or 0)
            if prior_dia and prior_dia.get("diamond_total_unrealized_present")
            else None
        )
        diamond_realized_since_ref = (
            float(dia.get("diamond_realized_since_ref") or 0)
            if dia and dia.get("diamond_realized_since_ref_present")
            else 0.0
        )
        if pos_T is not None:
            diamond_pnl = compute_daily_pnl(
                position_T=pos_T,
                position_prior=pos_prior,
                trades_for_symbol=trades_for_symbol,
                bond_like=bond_like,
                option_like=option_like,
                futures_like=futures_like,
                futures_option_like=futures_option_like,
            )
            mismatch = audit_trade_quantity_continuity(
                pos_T,
                pos_prior,
                trades_for_symbol,
                match_key=key,
                ticker=pick_display_ticker(psc, dia) or key,
            )
            if mismatch:
                trade_qty_mismatches.append(mismatch)
        elif diamond_total_unrealized is not None and diamond_prior_total_unrealized is not None:
            diamond_pnl = round(
                diamond_total_unrealized
                - diamond_prior_total_unrealized
                + diamond_realized_since_ref,
                2,
            )
        elif dia and (
            dia.get("diamond_unrealized_since_ref_present")
            or dia.get("diamond_realized_since_ref_present")
        ):
            # Fallback when prior snapshot / trades unavailable.
            diamond_pnl = round(
                float(dia.get("diamond_unrealized_since_ref") or 0)
                + float(dia.get("diamond_realized_since_ref") or 0),
                2,
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
        diamond_dividends = (
            round(float(dia.get("diamond_dividends") or 0), 2)
            if dia and dia.get("diamond_dividends_present")
            else None
        )
        alphadesk_dividends = (
            round(float(psc.get("alphadesk_dividends") or 0), 2)
            if psc and psc.get("alphadesk_dividends_present")
            else None
        )
        dividend_difference = (
            round(diamond_dividends - alphadesk_dividends, 2)
            if diamond_dividends is not None and alphadesk_dividends is not None
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
                "diamond_total_unrealized": diamond_total_unrealized,
                "diamond_prior_total_unrealized": diamond_prior_total_unrealized,
                "diamond_realized_since_reference": diamond_realized_since_ref,
                "alphadesk_pnl": alphadesk_pnl,
                "alphadesk_pnl_local": alphadesk_pnl_local,
                "alphadesk_sec_ccy": psc.get("sec_ccy") if psc else None,
                "alphadesk_portfolio_ccy": psc.get("portfolio_ccy") if psc else None,
                "alphadesk_fx_settle_to_base": psc.get("fx_settle_to_base") if psc else None,
                "pnl_difference": pnl_difference,
                "diamond_dividends": diamond_dividends,
                "alphadesk_dividends": alphadesk_dividends,
                "dividend_difference": dividend_difference,
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
    meta["lines_with_dividend_diff"] = sum(
        1
        for r in lines
        if r.get("dividend_difference") is not None and abs(float(r["dividend_difference"])) > 0.01
    )
    meta["total_dividend_difference"] = round(
        sum(
            float(r["dividend_difference"] or 0)
            for r in lines
            if r.get("dividend_difference") is not None
        ),
        2,
    )
    if trade_qty_mismatches:
        meta["trade_qty_mismatches"] = trade_qty_mismatches
    return lines, meta


_DEBUG_POSITION_FIELDS = (
    "CompanySymbol",
    "PricingTicker",
    "SecurityName",
    "SecurityType",
    "LongShort",
    "QuoteDate",
    "PortfolioPrice",
    "Quantity",
    "QuantityMultiplier",
    "BaseTotalUnrealizedGainLoss",
    "BaseUnrealizedGainLossSinceReferenceDate",
    "BaseUnrealizedGainLossSinceReferenceDateDueToPriceChange",
    "BaseUnrealizedGainLossSinceReferenceDateDueToFX",
    "BaseRealizedGainLossSinceReferenceDate",
    "BaseDividendIncomeSinceReferenceDate",
    "LocalMarketValue",
    "BaseMarketValue",
    "PriceSource",
    "QuoteType",
)

_DEBUG_TRADE_FIELDS = (
    "Ticker",
    "SecurityName",
    "ValuationDate",
    "TradeDate",
    "SettlementDate",
    "TradeType",
    "Quantity",
    "LocalAmountPerShare",
    "BaseNetAmount",
    "IsReversal",
    "IsBackDated",
    "IsCancel",
)


def resolve_close_price_fund_id(fund_or_id: str) -> str:
    """Map fund display name or GUID to Diamond fund parent id."""
    raw = (fund_or_id or "").strip()
    if not raw:
        return ""
    key = re.sub(r"\s+", " ", raw.upper())
    if key in FUND_NAME_TO_ID:
        return FUND_NAME_TO_ID[key]
    for name, fid in FUND_NAME_TO_ID.items():
        if key in name or name.startswith(key):
            return fid
    return raw


def _debug_ticker_matches(value: Any, ticker: str) -> bool:
    text = _norm(value).upper()
    if not text:
        return False
    ticker_u = ticker.upper()
    if text == ticker_u:
        return True
    if text.startswith(f"{ticker_u} "):
        return True
    if f" {ticker_u} " in f" {text} ":
        return True
    return False


def _record_matches_debug_ticker(record: Dict[str, Any], ticker: str) -> bool:
    ticker_u = ticker.upper()
    root = ticker_u.split(".")[0] if "." in ticker_u else ticker_u
    for field in (
        "CompanySymbol",
        "PricingTicker",
        "SecurityName",
        "Symbol",
        "Ticker",
        "SEDOL",
        "CUSIP",
        "ISIN",
    ):
        val = _norm(record.get(field)).upper()
        if not val:
            continue
        if val == ticker_u or val == root:
            return True
        if field in ("PricingTicker", "Ticker", "CompanySymbol"):
            token = val.split()[0] if " " in val else val
            if token == root:
                return True
        if _debug_ticker_matches(val, ticker_u):
            return True
    return False


def _summarize_debug_record(record: Dict[str, Any], fields: Tuple[str, ...]) -> Dict[str, Any]:
    out = {field: record.get(field) for field in fields if field in record}
    out["_full_record"] = record
    return out


def _d(value: Any) -> Decimal:
    """Safe decimal parse — Diamond fields arrive as strings or floats."""
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _trade_date_iso(trade: Dict[str, Any]) -> str:
    raw = _norm(trade.get("TradeDate") or trade.get("ValuationDate"))
    return raw[:10] if len(raw) >= 10 else raw


def _position_base_income(row: Optional[Dict[str, Any]]) -> Decimal:
    if not row:
        return Decimal("0")
    dividend = _d(row.get("BaseDividendIncomeSinceReferenceDate"))
    interest = _d(row.get("BaseInterestIncomeSinceReferenceDate"))
    withholding = _d(row.get("BaseWithholdingTaxSinceReferenceDate"))
    return dividend + interest - withholding


def _position_fx_to_base(row: Dict[str, Any]) -> Decimal:
    local_mv = _d(row.get("LocalMarketValue"))
    base_mv = _d(row.get("BaseMarketValue"))
    if local_mv != 0:
        return base_mv / local_mv
    return Decimal("1")


def _mtm_trade_price(
    trade_price: Decimal,
    position_T: Dict[str, Any],
    *,
    bond_like: bool,
) -> Decimal:
    if not bond_like:
        return trade_price
    sec_name = position_T.get("SecurityName")
    sec_type = position_T.get("SecurityType") or position_T.get("AssetType")
    pricing = position_T.get("PricingTicker")
    normalized = normalize_diamond_close_price(
        float(trade_price),
        security_name=sec_name,
        security_type=sec_type,
        bbg_ticker=pricing,
        is_bond_like=True,
    )
    return _d(normalized if normalized is not None else trade_price)


def _position_price_multiplier_for_mtm(
    row: Dict[str, Any],
    *,
    bond_like: bool,
    option_like: bool,
    futures_like: bool,
    futures_option_like: bool = False,
) -> Decimal:
    if bond_like:
        return Decimal("0.01")
    if futures_option_like:
        qm = _nonzero_amount(row.get("QuantityMultiplier"))
        if qm is not None:
            return _d(qm)
        return Decimal("1000")
    if futures_like:
        return _d(diamond_futures_contract_multiplier(row))
    sec_type = row.get("SecurityType") or row.get("AssetType")
    pricing = row.get("PricingTicker") or row.get("SecurityName")
    mult = notional_quantity_multiplier(sec_type, pricing)
    return _d(mult)


def _position_close_for_mtm(
    row: Dict[str, Any],
    *,
    bond_like: bool,
    option_like: bool,
    futures_like: bool,
) -> Decimal:
    sec_name = row.get("SecurityName")
    sec_type = row.get("SecurityType") or row.get("AssetType")
    pricing = row.get("PricingTicker")
    if futures_like:
        return _d(normalize_diamond_futures_close(row))
    return _d(
        normalize_diamond_close_price(
            row.get("PortfolioPrice"),
            security_name=sec_name,
            security_type=sec_type,
            bbg_ticker=pricing,
            is_bond_like=bond_like,
            is_option_like=option_like,
            price_discount=row.get("PriceDiscount"),
            pre_discount_price=row.get("PreDiscountPrice"),
        )
    )


def _merge_position_legs(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    if len(rows) == 1:
        return rows[0]
    net_qty = sum(_diamond_signed_qty(row) for row in rows)
    main = max(rows, key=lambda row: abs(_diamond_signed_qty(row)))
    merged = dict(main)
    merged["Quantity"] = float(net_qty)
    return merged


def _index_diamond_positions_by_match_key(
    records: List[Dict[str, Any]],
    valuation_date_iso: str,
) -> Dict[str, Dict[str, Any]]:
    use_rows = _diamond_rows_for_valuation_date(records, valuation_date_iso)
    underlying_index = build_underlying_ticker_index(records)
    legs: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in use_rows:
        key = _diamond_match_key(row, underlying_index=underlying_index)
        if not key:
            continue
        legs[key].append(row)
    out: Dict[str, Dict[str, Any]] = {}
    for key, rows in legs.items():
        merged = _merge_position_legs(rows)
        if merged is not None:
            out[key] = merged
    return out


def _trade_match_key(
    trade: Dict[str, Any],
    *,
    underlying_index: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    opt_root = ""
    bbg = _norm(trade.get("BBGID"))
    if underlying_index and bbg and bbg in underlying_index:
        opt_root = underlying_index[bbg]
    ticker = _norm(trade.get("Ticker"))
    if not opt_root and ticker and len(ticker) <= 12 and " " not in ticker:
        opt_root = ticker.upper()
    return reconcile_match_key(
        company_symbol=ticker or trade.get("SecurityName"),
        bbg_ticker=ticker,
        sedol=trade.get("SEDOL"),
        isin=trade.get("ISIN"),
        cusip=trade.get("CUSIP"),
        description=trade.get("SecurityName"),
        security_name=trade.get("SecurityName"),
        option_underlying_root=opt_root,
    )


def _diamond_trade_signed_contract_qty(trade: Dict[str, Any]) -> Decimal:
    """
    Signed trade quantity in the same units as Diamond position Quantity.

    Equity option trades report Quantity in share units (contracts × 100).
    Futures and futures option trades use notional units (contracts × QuantityMultiplier,
    e.g. × 50 for ES, × 1000 for crude). Positions use contracts in both cases.
    """
    qty = _d(trade.get("Quantity"))
    trade_type = _norm(trade.get("TradeType")).upper()
    if "SELL" in trade_type and qty > 0:
        qty = -qty
    elif "BUY" in trade_type and qty < 0:
        qty = abs(qty)

    sec_name = trade.get("SecurityName")
    sec_type = trade.get("SecurityType") or trade.get("AssetType")
    ticker = trade.get("Ticker")
    if is_futures_option_like_position(
        security_type=sec_type,
        company_symbol=ticker,
        description=sec_name,
        bbg_ticker=ticker,
        security=sec_name,
        security_name=sec_name,
        cusip=trade.get("CUSIP"),
    ) or is_futures_like_position(
        security_type=sec_type,
        company_symbol=ticker,
        description=sec_name,
        bbg_ticker=ticker,
        security=sec_name,
        security_name=sec_name,
        cusip=trade.get("CUSIP"),
    ):
        qm = _nonzero_amount(trade.get("QuantityMultiplier"))
        if qm and qty != 0:
            qty = qty / Decimal(str(qm))
        return qty

    mult = notional_quantity_multiplier(sec_type, sec_name or ticker)
    if (mult == 100.0 or _is_diamond_option_trade(trade)) and qty != 0:
        qty = qty / Decimal("100")
    return qty


def build_trade_index_by_match_key(
    trades: List[Dict[str, Any]],
    valuation_date_iso: str,
    *,
    underlying_index: Optional[Dict[str, str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group same-day trades on valuation_date by reconcile match key.

    Each entry: qty (signed +buy / -sell), local trade price, fx to base.
    """
    valuation_date = normalize_valuation_date(valuation_date_iso)
    idx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        if str(trade.get("IsReversal") or "").lower() == "true":
            continue
        if str(trade.get("IsCancel") or "").lower() == "true":
            continue
        trade_date = _trade_date_iso(trade)
        if trade_date and trade_date != valuation_date:
            continue
        trade_type = _norm(trade.get("TradeType")).upper()
        if trade_type.startswith("CA-"):
            continue
        key = _trade_match_key(trade, underlying_index=underlying_index)
        if not key:
            continue
        qty = _diamond_trade_signed_contract_qty(trade)
        price = _d(trade.get("LocalAmountPerShare"))
        local_net = _d(trade.get("LocalNetAmount"))
        base_net = _d(trade.get("BaseNetAmount"))
        fx = Decimal("1")
        if local_net != 0:
            fx = abs(base_net / local_net)
        idx[key].append({"qty": qty, "price": price, "fx": fx})
    return idx


def compute_daily_pnl(
    position_T: Optional[Dict[str, Any]],
    position_prior: Optional[Dict[str, Any]],
    trades_for_symbol: List[Dict[str, Any]],
    *,
    bond_like: bool = False,
    option_like: bool = False,
    futures_like: bool = False,
    futures_option_like: bool = False,
    income_from_t_snapshot_only: bool = True,
) -> Optional[float]:
    """
    True daily MTM in base ccy from two Diamond snapshots plus same-day trades.

    pnl = (Price_T - Price_prior) * Qty_prior * mult * fx_T
        + sum((Price_T - TradePrice) * TradeQty * mult * fx_T)
        + income booked on T (dividend/coupon accrual on the T snapshot)
    """
    if position_T is None:
        return None

    price_T = _position_close_for_mtm(
        position_T,
        bond_like=bond_like,
        option_like=option_like,
        futures_like=futures_like,
    )
    if position_prior is not None:
        price_prior = _position_close_for_mtm(
            position_prior,
            bond_like=bond_like,
            option_like=option_like,
            futures_like=futures_like,
        )
        qty_prior = _d(_diamond_signed_qty(position_prior))
    else:
        price_prior = price_T
        qty_prior = Decimal("0")

    fx_T = _position_fx_to_base(position_T)
    mult = _position_price_multiplier_for_mtm(
        position_T,
        bond_like=bond_like,
        option_like=option_like,
        futures_like=futures_like,
        futures_option_like=futures_option_like,
    )

    mtm_carry = (price_T - price_prior) * qty_prior * mult * fx_T

    mtm_trades = Decimal("0")
    for trade in trades_for_symbol:
        trade_fx = trade.get("fx") or fx_T
        trade_price = _mtm_trade_price(_d(trade["price"]), position_T, bond_like=bond_like)
        mtm_trades += (price_T - trade_price) * trade["qty"] * mult * trade_fx

    if income_from_t_snapshot_only:
        income_delta = _position_base_income(position_T)
    else:
        income_delta = _position_base_income(position_T) - _position_base_income(position_prior)

    return round(float(mtm_carry + mtm_trades + income_delta), 2)


def audit_trade_quantity_continuity(
    position_T: Optional[Dict[str, Any]],
    position_prior: Optional[Dict[str, Any]],
    trades_for_symbol: List[Dict[str, Any]],
    *,
    match_key: str,
    ticker: str,
) -> Optional[Dict[str, Any]]:
    """Log when qty_T != qty_prior + sum(trade qty) — catches timezone / booking issues."""
    if not trades_for_symbol:
        return None
    qty_T = _d(_diamond_signed_qty(position_T)) if position_T else Decimal("0")
    qty_prior = _d(_diamond_signed_qty(position_prior)) if position_prior else Decimal("0")
    trade_sum = sum(trade["qty"] for trade in trades_for_symbol)
    expected = qty_prior + trade_sum
    if abs(qty_T - expected) <= Decimal("0.01"):
        return None
    return {
        "match_key": match_key,
        "ticker": ticker,
        "qty_prior": float(qty_prior),
        "trade_qty_sum": float(trade_sum),
        "qty_T": float(qty_T),
        "expected_qty_T": float(expected),
        "difference": float(qty_T - expected),
    }


def flatten_diamond_trade_records(raw: Any) -> List[Dict[str, Any]]:
    """Extract flat PortfolioTrade dicts from GetPortfolioTrades JSON."""
    if not isinstance(raw, dict):
        return []
    body = (
        raw.get("GetPortfolioTradesResponse")
        or raw.get("GetPortfolioTrades")
        or raw
    )
    if not isinstance(body, dict):
        return []
    trades = body.get("PortfolioTrades") or body.get("PortfolioTrade") or []
    if isinstance(trades, dict):
        inner = trades.get("PortfolioTrade")
        if isinstance(inner, dict):
            return [inner]
        if isinstance(inner, list):
            return [t for t in inner if isinstance(t, dict)]
        return [trades]
    if isinstance(trades, list):
        return [t for t in trades if isinstance(t, dict)]
    return []


def _portfolio_header_dates(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    body = raw.get("GetPortfolioResponse") or raw
    if not isinstance(body, dict):
        return {}
    return {
        key: body.get(key)
        for key in ("ReferenceDate", "ValuationDate", "FundParent", "ManagementCompanyCode")
        if body.get(key) is not None
    }


def build_close_price_debug_payload(
    fund_id: str,
    valuation_date_iso: str,
    debug_ticker: str,
    *,
    diamond_raw: Any,
    prior_diamond_raw: Any,
    reference_date_iso: Optional[str],
    trades_raw: Any = None,
    trades_error: Optional[str] = None,
) -> Dict[str, Any]:
    """TEMP: NAV checker debug bundle for a single ticker (JSON Diamond records, not XML)."""
    ticker = (debug_ticker or "").strip().upper()
    valuation_date_norm = normalize_valuation_date(valuation_date_iso)
    positions_t = [
        _summarize_debug_record(row, _DEBUG_POSITION_FIELDS)
        for row in flatten_diamond_portfolio_records(diamond_raw)
        if _record_matches_debug_ticker(row, ticker)
    ]
    positions_prior = [
        _summarize_debug_record(row, _DEBUG_POSITION_FIELDS)
        for row in flatten_diamond_portfolio_records(prior_diamond_raw)
        if _record_matches_debug_ticker(row, ticker)
    ]
    trades = [
        _summarize_debug_record(row, _DEBUG_TRADE_FIELDS)
        for row in flatten_diamond_trade_records(trades_raw)
        if _record_matches_debug_ticker(row, ticker)
    ]
    payload = {
        "ticker": ticker,
        "fund_id": fund_id,
        "valuation_date_T": valuation_date_norm,
        "valuation_date_prior": reference_date_iso,
        "portfolio_header_T": _portfolio_header_dates(diamond_raw),
        "portfolio_header_prior": _portfolio_header_dates(prior_diamond_raw),
        "positions_T": positions_t,
        "positions_prior": positions_prior,
        "trades_T": trades,
        "trades_error": trades_error,
        "note": (
            "Diamond API returns JSON (PortfolioRecordDetails / PortfolioTrade), not raw XML. "
            "_full_record on each item has all fields from the API."
        ),
    }
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"nav_checker_debug_{ticker}_{valuation_date_norm}.json"
        log_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        payload["debug_log_path"] = str(log_path)
    except OSError:
        payload["debug_log_path"] = None
    return payload


def fetch_close_price_reconciliation(
    fund_id: str,
    valuation_date_iso: str,
    diamond_client: Any,
    *,
    debug_ticker: Optional[str] = None,
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
    prior_raw = None
    prior_error = None
    trades_raw = None
    trades_error = None
    if reference_date:
        try:
            prior_raw = diamond_client.get_portfolio(
                fund_id=fund_id,
                valuation_date=reference_date,
            )
        except Exception as exc:
            prior_error = str(exc)
        try:
            trades_raw = diamond_client.get_portfolio_trades(
                fund_parent_id=fund_id,
                start_date=reference_date,
                end_date=valuation_date_norm,
                date_type="ValuationDate",
            )
        except Exception as exc:
            trades_error = str(exc)
    lines, meta = build_close_price_reconciliation(
        fund_id,
        valuation_date_norm,
        raw,
        prior_diamond_raw=prior_raw,
        reference_date_iso=reference_date,
        trades_raw=trades_raw,
    )
    meta["fund_id"] = fund_id
    meta["valuation_date"] = valuation_date_norm
    meta["diamond_reference_date"] = reference_date
    meta["diamond_reference_date_source"] = reference_source
    if reference_portfolio:
        meta["diamond_reference_psc_portfolio"] = reference_portfolio
    if reference_error:
        meta["diamond_reference_warning"] = reference_error
    if prior_error:
        meta["diamond_prior_warning"] = prior_error
    if trades_error:
        meta["diamond_trades_warning"] = trades_error
    if debug_ticker:
        meta["debug"] = build_close_price_debug_payload(
            fund_id,
            valuation_date_norm,
            debug_ticker,
            diamond_raw=raw,
            prior_diamond_raw=prior_raw,
            reference_date_iso=reference_date,
            trades_raw=trades_raw,
            trades_error=trades_error,
        )
    return lines, meta, None
