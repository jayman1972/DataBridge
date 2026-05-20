"""Parse SGGG Diamond GetNAVSheet responses into per-class NAV / return summaries."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _return_value_to_bps(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        if abs(v) <= 1.5:
            return int(round(v * 10_000))
        return int(round(v * 100))
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("%"):
        try:
            return int(round(float(s[:-1].strip()) * 100))
        except ValueError:
            return None
    try:
        v = float(s.replace(",", ""))
        if abs(v) <= 1.5:
            return int(round(v * 10_000))
        return int(round(v * 100))
    except ValueError:
        return None


# Native fund currency for Fund Admin NAV checker (compliance Steps are USD for alts).
FUND_NATIVE_CURRENCY: Dict[str, str] = {
    "415a3530-3034-4536-4432-303030364337": "USD",
    "41010000-7F7A-0A65-D559-45484608DB40": "USD",
    "41323030-3031-4144-3637-303030364338": "USD",
    "41010000-7F2A-D7E8-776F-45484608D91C": "USD",
    "01010000-801A-4995-8370-45484608DE57": "CAD",
}


def _section_items_by_name(node: Any) -> Dict[str, Any]:
    """Collect SectionItem Name->Value from a NAV sheet node (fund or class)."""
    out: Dict[str, Any] = {}
    if not isinstance(node, dict):
        return out
    for sec in node.get("SectionList") or []:
        if not isinstance(sec, dict):
            continue
        for item in sec.get("SectionItem") or []:
            if not isinstance(item, dict):
                continue
            name = (item.get("Name") or "").strip()
            if name:
                out[name] = item.get("Value")
    return out


def _parse_money_value(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().replace(",", "").replace("$", "")
    if not s or s in ("-", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def pick_native_net_asset_value(
    body: Dict[str, Any],
    fund_id: str,
) -> Tuple[Optional[float], str]:
    """
    Prefer USD Net Asset Value from Diamond NAV sheet when fund is USD-denominated.
    Returns (amount, currency_code).
    """
    native = FUND_NATIVE_CURRENCY.get(fund_id, "USD")
    fund_ccy = (body.get("FundCurrency") or "").strip().upper()
    items = _section_items_by_name(body)

    usd_keys = (
        "Net Asset Value (USD)",
        "Net Asset Value USD",
        "Total Net Assets (USD)",
        "Net Assets (USD)",
    )
    cad_keys = (
        "Net Asset Value (CAD)",
        "Net Asset Value CAD",
        "Total Net Assets (CAD)",
    )

    if native == "USD":
        for key in usd_keys:
            v = _parse_money_value(items.get(key))
            if v is not None:
                return v, "USD"
        nav = _parse_money_value(body.get("NetAssetValue"))
        if nav is not None and fund_ccy in ("", "USD", "US Dollar", "US$"):
            return nav, "USD"
        for key in cad_keys:
            v = _parse_money_value(items.get(key))
            if v is not None:
                return v, "CAD"

    if native == "CAD":
        for key in cad_keys:
            v = _parse_money_value(items.get(key))
            if v is not None:
                return v, "CAD"
        nav = _parse_money_value(body.get("NetAssetValue"))
        if nav is not None:
            return nav, "CAD" if fund_ccy != "USD" else "USD"

    nav = _parse_money_value(body.get("NetAssetValue"))
    return nav, native if nav is not None else native


def pick_capital_flow_adjustment(body: Dict[str, Any]) -> Optional[float]:
    """
    Net subscriptions/redemptions on the valuation date from Diamond NAV sheet, if present.
    Positive = net inflow (same sign as compliance Net Subs (reds)).
    """
    items = _section_items_by_name(body)
    for subs_key, reds_key in (
        ("Subscriptions", "Redemptions"),
        ("Subscription", "Redemption"),
        ("Net Subscriptions", None),
        ("Net Subs (reds)", None),
    ):
        if reds_key:
            subs = _parse_money_value(items.get(subs_key)) or 0.0
            reds = _parse_money_value(items.get(reds_key)) or 0.0
            if subs != 0 or reds != 0:
                return subs - abs(reds) if reds < 0 else subs + reds
        else:
            net = _parse_money_value(items.get(subs_key))
            if net is not None:
                return net
    return None


def _valuation_period_return(class_entry: Dict[str, Any]) -> Any:
    for sec in class_entry.get("SectionList") or []:
        if sec.get("SectionName") != "Returns":
            continue
        for item in sec.get("SectionItem") or []:
            if item.get("Name") == "Valuation Period Return":
                return item.get("Value")
    return None


def parse_nav_sheet_summary(payload: Any) -> Dict[str, Any]:
    """
    Normalize GetNAVSheet JSON into fund-level summary.

    Returns dict with keys: fund_parent_id, fund_currency, valuation_date,
    net_asset_value, classes (list), available (bool).
    """
    if not isinstance(payload, dict):
        return {"available": False, "classes": [], "error": "Invalid NAV sheet payload"}

    body = payload.get("GetNAVSheetResponse") if "GetNAVSheetResponse" in payload else payload
    if not isinstance(body, dict):
        return {"available": False, "classes": [], "error": "Missing GetNAVSheetResponse"}

    raw_classes = body.get("ClassSeriesFundList")
    if not isinstance(raw_classes, list) or len(raw_classes) == 0:
        return {
            "available": False,
            "fund_parent_id": body.get("FundParentID"),
            "fund_currency": body.get("FundCurrency"),
            "valuation_date": body.get("ValuationDate"),
            "net_asset_value": body.get("NetAssetValue"),
            "classes": [],
        }

    classes_out: List[Dict[str, Any]] = []
    for entry in raw_classes:
        if not isinstance(entry, dict):
            continue
        class_id = (entry.get("FundID") or "").strip()
        if not class_id:
            continue
        navpu = entry.get("NAVPU")
        ret_raw = _valuation_period_return(entry)
        classes_out.append(
            {
                "class_id": class_id,
                "class_code": (entry.get("ClassCode") or "").strip() or None,
                "navpu": float(navpu) if navpu is not None else None,
                "bps": _return_value_to_bps(ret_raw),
                "return_display": str(ret_raw).strip() if ret_raw is not None else None,
            }
        )

    fund_id = (body.get("FundParentID") or "").strip()
    nav_native, nav_ccy = pick_native_net_asset_value(body, fund_id)
    capital_flow = pick_capital_flow_adjustment(body)

    has_nav = any(c.get("navpu") is not None for c in classes_out)
    return {
        "available": has_nav and len(classes_out) > 0,
        "fund_parent_id": body.get("FundParentID"),
        "fund_currency": body.get("FundCurrency"),
        "valuation_date": body.get("ValuationDate"),
        "net_asset_value": body.get("NetAssetValue"),
        "net_asset_value_native": nav_native,
        "native_currency": nav_ccy,
        "capital_flow": capital_flow,
        "classes": sorted(classes_out, key=lambda x: x.get("class_id") or ""),
    }


# Diamond fund parent GUID -> PSC portfolio name (Fund Admin NAV checker)
NAV_CHECKER_FUND_ID_TO_PSC: Dict[str, str] = {
    "415a3530-3034-4536-4432-303030364337": "EHP Alpha",
    "41010000-7F7A-0A65-D559-45484608DB40": "EHP Tact Growth Alt",
    "41323030-3031-4144-3637-303030364338": "EHP Select Alt",
    "41010000-7F2A-D7E8-776F-45484608D91C": "EHP Strat Inc Alt",
    "01010000-801A-4995-8370-45484608DE57": "Exponential Balanced Growth Fund",
}


def _compact_yyyymmdd(iso_date: str) -> str:
    return normalize_valuation_date(iso_date).replace("-", "")


def fetch_psc_portfolio_navs(
    fund_ids: List[str],
    prior_date_iso: str,
    valuation_date_iso: str,
    dsn: str = "PSC_VIEWER",
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Fund-level PORTFOLIO_NAV from PSC for prior and valuation dates.
    Returns {fund_id: {"opening": float|None, "closing": float|None}}.
    """
    portfolios = {
        fid: NAV_CHECKER_FUND_ID_TO_PSC.get(fid)
        for fid in fund_ids
        if NAV_CHECKER_FUND_ID_TO_PSC.get(fid)
    }
    if not portfolios:
        return {}

    prior_compact = _compact_yyyymmdd(prior_date_iso)
    val_compact = _compact_yyyymmdd(valuation_date_iso)
    port_names = sorted(set(portfolios.values()))
    port_placeholders = ",".join(["?"] * len(port_names))

    try:
        import pyodbc
    except ImportError:
        return {}

    out: Dict[str, Dict[str, Optional[float]]] = {fid: {"opening": None, "closing": None} for fid in fund_ids}
    conn = None
    try:
        conn = pyodbc.connect(f"DSN={dsn}", timeout=15)
        cur = conn.cursor()
        sql = (
            "SELECT PORTFOLIO, POSN_DATE_INT, MAX(PORTFOLIO_NAV) AS NAV "
            "FROM psc_position_history "
            f"WHERE PORTFOLIO IN ({port_placeholders}) AND POSN_DATE_INT IN (?, ?) "
            "GROUP BY PORTFOLIO, POSN_DATE_INT"
        )
        cur.execute(sql, (*port_names, prior_compact, val_compact))
        nav_by_port_date: Dict[tuple, float] = {}
        for row in cur.fetchall() or []:
            port = (str(row[0]).strip() if row[0] is not None else "")
            dt = (str(row[1]).strip() if row[1] is not None else "")
            nav = float(row[2]) if row[2] is not None else None
            if port and dt and nav is not None:
                nav_by_port_date[(port, dt)] = nav

        for fid, port in portfolios.items():
            if not port:
                continue
            opening = nav_by_port_date.get((port, prior_compact))
            closing = nav_by_port_date.get((port, val_compact))
            out[fid] = {
                "opening": opening,
                "closing": closing,
            }
    except Exception:
        return {fid: {"opening": None, "closing": None} for fid in fund_ids}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return out


def prior_business_day_iso(valuation_date: str) -> str:
    """Previous business day relative to yyyy-mm-dd valuation date."""
    from datetime import date, timedelta

    d = date.fromisoformat(normalize_valuation_date(valuation_date))
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    return d.isoformat()


def pick_class_i_bps(classes: List[Dict[str, Any]]) -> Optional[int]:
    """Return valuation-period return (bps) for Class I share class."""
    for cls in classes or []:
        cid = (cls.get("class_id") or "").strip()
        code = (cls.get("class_code") or "").strip()
        if cid.upper() in ("I", "CLASS I") or code.upper() in ("I", "CLASS I"):
            return cls.get("bps")
        if re.match(r"^class\s*i\b", cid, re.IGNORECASE):
            return cls.get("bps")
    return None


def normalize_valuation_date(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise ValueError("valuation_date required")
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    compact = s.replace("-", "")[:8]
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    raise ValueError(f"Invalid valuation_date: {raw}")


_END_DATE_IN_MESSAGE = re.compile(r"End Date:\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE)
_NOT_FINALIZED_PHRASES = (
    "not yet been finalized",
    "has not been finalized",
    "not been finalized",
)


def nav_unavailable_user_message(end_date: str) -> str:
    return f"NAV not available yet for {end_date}"


def parse_diamond_nav_unavailable(error: Exception, valuation_date: str) -> Optional[Dict[str, str]]:
    """
    Detect Diamond HTTP 400 responses where the valuation period is not finalized yet.
    Returns {end_date, message} or None if this is a different error.
    """
    text = str(error or "")
    lower = text.lower()
    if "http 400" not in lower and " 400:" not in lower:
        return None
    if not any(phrase in lower for phrase in _NOT_FINALIZED_PHRASES):
        return None
    match = _END_DATE_IN_MESSAGE.search(text)
    end_date = match.group(1) if match else normalize_valuation_date(valuation_date)
    return {"end_date": end_date, "message": nav_unavailable_user_message(end_date)}
