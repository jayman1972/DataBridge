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
            f"({contract['underlying']} {underlying_price:g} vs {strike:g} {option_word} strike)"
        )
        risks.append(
            {
                "security": security,
                "security_display": format_option_contract(security),
                "net_contracts": net_contracts,
                "underlying": contract["underlying"],
                "underlying_bloomberg_ticker": quote.get("ticker"),
                "underlying_price": underlying_price,
                "expiration_date": contract["expiration_date"],
                "option_type": contract["option_type"],
                "strike": strike,
                "flag_reason": reason,
            }
        )
    return risks
