"""Pure helpers for option-expiry risk checks used by the closeout report."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional


_OCC_OPTION_RE = re.compile(
    r"^([A-Z0-9]{1,6})\s*(\d{2})(\d{2})(\d{2})([CP])(\d{8})$",
    re.IGNORECASE,
)


def parse_option_contract(value: Any) -> Optional[Dict[str, Any]]:
    """Parse a canonical OCC option key into its exercise terms."""
    text = str(value or "").strip().upper()
    match = _OCC_OPTION_RE.match(text)
    if not match:
        return None
    underlying, yy, mm, dd, option_type, strike_raw = match.groups()
    return {
        "underlying": underlying,
        "expiration_date": f"20{yy}-{mm}-{dd}",
        "option_type": option_type,
        "strike": int(strike_raw) / 1000.0,
    }


def format_option_contract(value: Any) -> str:
    """Format a canonical OCC option key for the report UI."""
    contract = parse_option_contract(value)
    if not contract:
        return str(value or "")
    yyyy, mm, dd = contract["expiration_date"].split("-")
    strike = float(contract["strike"])
    strike_text = (
        str(int(strike))
        if abs(strike - int(strike)) < 1e-9
        else f"{strike:.3f}".rstrip("0").rstrip(".")
    )
    return (
        f"{contract['underlying']} {mm}/{dd}/{yyyy[2:]} "
        f"{contract['option_type']}{strike_text}"
    )


def select_live_underlying_quote(
    quote: Dict[str, Any],
    expected_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Select a same-day live underlying quote without using prior close.

    Bloomberg's ``PX_OFFICIAL_CLOSE`` can remain yesterday's close until the
    current close is published. Same-day exercise checks must therefore use
    ``PX_LAST`` first, then a live bid/ask midpoint or ``PX_MID``. When
    Bloomberg supplies ``LAST_UPDATE_DT``, reject a quote from another date.
    """
    if not isinstance(quote, dict) or quote.get("error"):
        return None

    as_of_raw = quote.get("LAST_UPDATE_DT")
    as_of_match = re.search(r"\d{4}-\d{2}-\d{2}", str(as_of_raw or ""))
    as_of_date = as_of_match.group(0) if as_of_match else None
    if expected_date and as_of_date and as_of_date != expected_date:
        return None

    candidates = [("PX_LAST", quote.get("PX_LAST"))]
    try:
        bid = float(quote.get("PX_BID"))
        ask = float(quote.get("PX_ASK"))
        if bid > 0 and ask > 0:
            candidates.append(("PX_BID/PX_ASK", (bid + ask) / 2.0))
    except (TypeError, ValueError):
        pass
    candidates.append(("PX_MID", quote.get("PX_MID")))

    for field, value in candidates:
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return {"price": price, "price_field": field, "as_of_date": as_of_date}
    return None


def evaluate_expiring_itm_positions(
    open_positions: Iterable[Dict[str, Any]],
    report_date: str,
    underlying_quotes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return held options that expire on ``report_date`` and are strictly ITM.

    ``underlying_quotes`` is keyed by the option root and contains ``price`` plus
    the Bloomberg ticker used to obtain that price. Both long and short non-zero
    positions are included because either exercise or assignment can occur.
    """
    risks: List[Dict[str, Any]] = []
    for position in open_positions:
        security = str(position.get("security") or "").strip().upper()
        contract = parse_option_contract(security)
        if not contract or contract["expiration_date"] != report_date:
            continue

        try:
            net_contracts = float(position.get("net_contracts") or 0.0)
        except (TypeError, ValueError):
            continue
        if abs(net_contracts) <= 1e-9:
            continue

        quote = underlying_quotes.get(contract["underlying"]) or {}
        try:
            underlying_price = float(quote.get("price"))
        except (TypeError, ValueError):
            continue
        strike = float(contract["strike"])
        is_itm = (
            underlying_price > strike
            if contract["option_type"] == "C"
            else underlying_price < strike
        )
        if not is_itm:
            continue

        side = "Long" if net_contracts > 0 else "Short"
        contract_word = "contract" if abs(net_contracts) == 1 else "contracts"
        option_word = "call" if contract["option_type"] == "C" else "put"
        reason = (
            f"{side} {abs(net_contracts):g} {contract_word}; expires today in the money "
            f"and is subject to automatic exercise/assignment "
            f"({contract['underlying']} live {underlying_price:g} vs {strike:g} {option_word} strike)"
        )
        risks.append(
            {
                "security": security,
                "security_display": format_option_contract(security),
                "net_contracts": net_contracts,
                "underlying": contract["underlying"],
                "underlying_bloomberg_ticker": quote.get("ticker"),
                "underlying_price": underlying_price,
                "underlying_price_field": quote.get("price_field"),
                "underlying_price_as_of": quote.get("as_of_date"),
                "expiration_date": contract["expiration_date"],
                "option_type": contract["option_type"],
                "strike": strike,
                "flag_reason": reason,
            }
        )
    return risks
