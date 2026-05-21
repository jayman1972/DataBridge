"""Parse SGGG Diamond GetNAVSheet responses into per-class NAV / return summaries."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _return_value_to_bps(raw: Any, base_aum: Optional[float] = None) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
    else:
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
        except ValueError:
            return None
    if abs(v) <= 1.5:
        return int(round(v * 10_000))
    if base_aum is not None and abs(base_aum) > 0 and abs(v) < abs(base_aum):
        return int(round((v / float(base_aum)) * 10_000))
    return int(round(v * 100))


# Fund base / reporting currency (compliance Steps AUM and Diamond fund NAV are in this currency).
FUND_NATIVE_CURRENCY: Dict[str, str] = {
    "415a3530-3034-4536-4432-303030364337": "CAD",
    "41010000-7F7A-0A65-D559-45484608DB40": "CAD",
    "41323030-3031-4144-3637-303030364338": "CAD",
    "41010000-7F2A-D7E8-776F-45484608D91C": "CAD",
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


def _normalize_currency_code(raw: Any, default: str = "CAD") -> str:
    s = (str(raw or "")).strip().upper()
    if not s:
        return default
    if "USD" in s or s in ("US$", "US"):
        return "USD"
    if "CAD" in s or "CAN" in s:
        return "CAD"
    return default


_SERIES_CODE_RE = re.compile(r"^[A-Z0-9]{2,8}$")

# Section labels that are not closing NAVPU (often prior-day or change fields).
_NAVPU_SKIP_NAME_FRAGMENTS = (
    "PRIOR",
    "PREVIOUS",
    "OPENING",
    "BEGIN",
    "CHANGE",
    "DELTA",
    "VARIANCE",
    "DIFFERENCE",
    "RETURN",
    "BPS",
    "%",
)


def _class_series_token(class_code: str, class_id: str) -> str:
    """Series label (e.g. 500UO, 200I) — not a Diamond parent-fund GUID."""
    for raw in (class_code, class_id):
        token = (raw or "").strip().upper()
        if _SERIES_CODE_RE.match(token):
            return token
    code = (class_code or "").strip().upper()
    if _SERIES_CODE_RE.match(code):
        return code
    return ""


def _is_usd_share_class(class_code: str, class_id: str) -> bool:
    """USD series codes include U (500UO, 550UF). Ignore GUIDs that happen to contain U."""
    series = _class_series_token(class_code, class_id)
    if not series:
        return False
    return "U" in series


def _is_navpu_section_name(name: str) -> bool:
    upper = (name or "").upper()
    if any(frag in upper for frag in _NAVPU_SKIP_NAME_FRAGMENTS):
        return False
    return "NAV" in upper or "PRICE" in upper or "UNIT" in upper or " PU" in upper or upper.endswith("PU")


def _navpu_from_section_items(
    items: Dict[str, Any],
    *,
    want_usd: bool,
) -> Optional[float]:
    """Scan NAV sheet section labels for currency-specific closing NAVPU."""
    preferred: List[float] = []
    fallback: List[float] = []
    for name, raw in items.items():
        if not _is_navpu_section_name(name):
            continue
        upper = (name or "").upper()
        val = _parse_money_value(raw)
        if val is None or val <= 0:
            continue
        if want_usd:
            if "USD" in upper or "U.S." in upper:
                preferred.append(val)
            elif "CAD" not in upper and "CAN" not in upper:
                fallback.append(val)
        else:
            if "CAD" in upper or "CAN" in upper:
                preferred.append(val)
            elif "USD" not in upper:
                fallback.append(val)
    if preferred:
        return preferred[0]
    return fallback[0] if fallback else None


def _entry_navpu(entry: Dict[str, Any], *, want_usd: bool) -> Optional[float]:
    """Top-level NAVPU on the class entry (authoritative when present)."""
    if want_usd:
        keys = ("NAVPU", "Price", "NAV per Unit (USD)", "NAV Per Unit (USD)")
    else:
        keys = ("NAVPU", "Price", "NAV per Unit", "NAV Per Unit")
    for key in keys:
        val = _parse_money_value(entry.get(key))
        if val is not None and val > 0:
            return val
    return None


def _parse_class_navpu(entry: Dict[str, Any], fund_base_ccy: str) -> Tuple[Optional[float], str]:
    """
    Per-class NAVPU in the class's own currency. USD series (code contains U) use USD NAVPU fields.
    """
    class_code = (entry.get("ClassCode") or "").strip()
    class_id = (entry.get("FundID") or "").strip()
    raw_ccy = entry.get("Currency") or entry.get("ClassCurrency")
    if raw_ccy:
        class_ccy = _normalize_currency_code(raw_ccy, default=fund_base_ccy)
    elif _is_usd_share_class(class_code, class_id):
        class_ccy = "USD"
    else:
        class_ccy = fund_base_ccy

    want_usd = class_ccy == "USD"
    navpu = _entry_navpu(entry, want_usd=want_usd)
    if navpu is None:
        items = _section_items_by_name(entry)
        navpu = _navpu_from_section_items(items, want_usd=want_usd)

    return navpu, class_ccy


def normalize_diamond_sheet_date(raw: Any) -> Optional[str]:
    """Normalize ValuationDate from GetNAVSheet response to yyyy-mm-dd."""
    if raw is None:
        return None
    if hasattr(raw, "date"):
        try:
            return raw.date().isoformat()
        except Exception:
            pass
    s = str(raw).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    compact = s.replace("-", "")[:8]
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    return None


def fund_aum_from_summary(summary: Dict[str, Any]) -> Optional[float]:
    """Fund-level AUM from a parsed GetNAVSheet summary (for cache / SGGG day change)."""
    nav = summary.get("net_asset_value_native")
    if nav is not None:
        return float(nav)
    return _parse_money_value(summary.get("net_asset_value"))


def sum_class_net_assets_cad(body: Dict[str, Any], fund_id: str) -> Optional[float]:
    """Sum per-class net assets for CAD series only (USD series excluded)."""
    base = FUND_NATIVE_CURRENCY.get(fund_id, "CAD")
    raw_classes = body.get("ClassSeriesFundList")
    if not isinstance(raw_classes, list):
        return None
    total = 0.0
    found = 0
    for entry in raw_classes:
        if not isinstance(entry, dict):
            continue
        class_code = (entry.get("ClassCode") or "").strip()
        class_id = (entry.get("FundID") or "").strip()
        if base == "CAD" and _is_usd_share_class(class_code, class_id):
            continue
        items = _section_items_by_name(entry)
        class_nav = None
        for key in (
            "Net Asset Value (CAD)",
            "Net Asset Value CAD",
            "Total Net Assets (CAD)",
            "Net Assets (CAD)",
            "Net Asset Value",
            "Total Net Assets",
            "Net Assets",
        ):
            class_nav = _parse_money_value(items.get(key))
            if class_nav is not None:
                break
        if class_nav is None:
            class_nav = _parse_money_value(entry.get("NetAssetValue"))
        if class_nav is not None:
            total += class_nav
            found += 1
    return total if found else None


def _is_capital_flow_item(name: str) -> bool:
    """True for subscription/redemption dollar lines (not NAV-before-fee rows)."""
    upper = (name or "").upper()
    if not upper:
        return False
    if "NET ASSET" in upper or " BEFORE " in upper or "NAVPU" in upper:
        return False
    # Diamond class-level lines (Alpha May 2026): positive = inflow, negative = outflow.
    if re.search(r"ADJUSTED\s+OPENING\s+EQUITY\s+(CONTRIBUTIONS|REDEMPTIONS)", upper):
        return True
    if re.search(r"UNITS\s+(CONTRIBUTIONS|REDEMPTIONS)", upper):
        return True
    if "SUBSCRIPTION" in upper or "CONTRIBUTION" in upper:
        return "EQUITY" in upper or "UNITS" in upper or upper in ("SUBSCRIPTIONS", "SUBSCRIPTION")
    if "REDEMPTION" in upper or "WITHDRAWAL" in upper:
        return "EQUITY" in upper or "UNITS" in upper or upper in ("REDEMPTIONS", "REDEMPTION")
    if "NET SUBS" in upper or "NET CAPITAL" in upper or "CAPITAL FLOW" in upper:
        return True
    return False


def list_capital_flow_candidates(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """All subs/redemption-like lines on the NAV sheet, every share class (for debugging / UI)."""
    out: List[Dict[str, Any]] = []
    for scope, node in (("fund", body),):
        items = _section_items_by_name(node)
        for name, raw in sorted(items.items()):
            if not _is_capital_flow_item(name):
                continue
            val = _parse_money_value(raw)
            if val is not None:
                out.append({"scope": scope, "class_code": None, "name": name, "amount": val})
    for entry in body.get("ClassSeriesFundList") or []:
        if not isinstance(entry, dict):
            continue
        code = (entry.get("ClassCode") or "").strip() or None
        for sec in entry.get("SectionList") or []:
            sec_name = (sec.get("SectionName") or "").strip()
            for item in sec.get("SectionItem") or []:
                name = (item.get("Name") or "").strip()
                if not _is_capital_flow_item(name):
                    continue
                val = _parse_money_value(item.get("Value"))
                if val is not None:
                    out.append(
                        {
                            "scope": "class",
                            "class_code": code,
                            "section": sec_name or None,
                            "name": name,
                            "amount": val,
                        }
                    )
    return out


def pick_fund_aum_for_role(
    body: Dict[str, Any],
    fund_id: str,
    role: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Fund AUM from GetNAVSheet for opening vs closing on the sheet's valuation date.
    role: 'closing' (EOD / end of period) or 'opening' (start of period).
    """
    items = _section_items_by_name(body)
    root_nav = _parse_money_value(body.get("NetAssetValue"))

    if role == "closing":
        for key in (
            "Closing Net Asset Value",
            "Closing Net Asset Value (CAD)",
            "Ending Net Asset Value",
            "Net Asset Value (CAD)",
            "Net Asset Value",
            "Total Net Assets",
            "Fund Net Asset Value",
        ):
            v = _parse_money_value(items.get(key))
            if v is not None:
                return v, key
        if root_nav is not None:
            return root_nav, "NetAssetValue"
        class_sum = sum_class_net_assets_cad(body, fund_id)
        if class_sum is not None:
            return class_sum, "class_sum_cad"
        return None, None

    for key in (
        "Opening Net Asset Value",
        "Opening Net Asset Value (CAD)",
        "Beginning Net Asset Value",
        "Opening Net Assets",
        "Beginning Net Assets",
    ):
        v = _parse_money_value(items.get(key))
        if v is not None:
            return v, key
    for name, raw in items.items():
        upper = (name or "").upper()
        if "OPENING" in upper or "BEGINNING" in upper:
            if "NET ASSET" in upper or "NAV" in upper:
                v = _parse_money_value(raw)
                if v is not None:
                    return v, name
    if root_nav is not None:
        return root_nav, "NetAssetValue"
    class_sum = sum_class_net_assets_cad(body, fund_id)
    if class_sum is not None:
        return class_sum, "class_sum_cad"
    return None, None


def pick_fund_net_asset_value(
    body: Dict[str, Any],
    fund_id: str,
) -> Tuple[Optional[float], str]:
    """Fund-level NetAssetValue in the fund's base currency (typically CAD)."""
    base = FUND_NATIVE_CURRENCY.get(fund_id, "CAD")
    fund_ccy = _normalize_currency_code(body.get("FundCurrency"), default=base)
    items = _section_items_by_name(body)

    for key in (
        "Net Asset Value (CAD)",
        "Net Asset Value CAD",
        "Total Net Assets (CAD)",
        "Net Assets (CAD)",
        "Net Asset Value",
        "Total Net Assets",
        "Fund Net Asset Value",
        "Net Assets",
    ):
        v = _parse_money_value(items.get(key))
        if v is not None:
            ccy = "CAD" if "CAD" in key.upper() else fund_ccy if fund_ccy in ("CAD", "USD") else base
            return v, ccy

    nav = _parse_money_value(body.get("NetAssetValue"))
    if nav is not None:
        return nav, fund_ccy if fund_ccy in ("CAD", "USD") else base
    return None, base


def pick_capital_flow_adjustment(body: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    Net subscriptions/redemptions on the valuation date from Diamond NAV sheet.
    Positive = net inflow (same sign as compliance Net Subs (reds)).
    Sums class-level flow lines across all entries in ClassSeriesFundList (any class:
    A, F, I, O, UA, UO, etc.) — not Class I only.
    """
    items = _section_items_by_name(body)
    for subs_key, reds_key in (
        ("Subscriptions", "Redemptions"),
        ("Subscription", "Redemption"),
        ("Net Subscriptions", None),
        ("Net Subs (reds)", None),
        ("Net Subscriptions/(Redemptions)", None),
        ("Net Subscriptions / Redemptions", None),
    ):
        if reds_key:
            subs = _parse_money_value(items.get(subs_key)) or 0.0
            reds = _parse_money_value(items.get(reds_key)) or 0.0
            if subs != 0 or reds != 0:
                net = subs + reds
                return net, f"{subs_key} / {reds_key}"
        else:
            net = _parse_money_value(items.get(subs_key))
            if net is not None:
                return net, subs_key

    total = 0.0
    labels: List[str] = []
    for row in list_capital_flow_candidates(body):
        if row.get("scope") != "class":
            continue
        amt = float(row["amount"])
        if amt == 0:
            continue
        total += amt
        code = row.get("class_code") or "?"
        labels.append(f"{code}:{row.get('name')}")
    if labels:
        label = "; ".join(labels[:6]) + ("…" if len(labels) > 6 else "")
        n_class = len({row.get("class_code") for row in list_capital_flow_candidates(body) if row.get("scope") == "class"})
        if n_class > 1:
            label = f"all classes ({n_class}): {label}"
        return total, label
    return None, None


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

    fund_id_early = (body.get("FundParentID") or "").strip()
    nav_native_early, nav_ccy_early = pick_fund_net_asset_value(body, fund_id_early)

    raw_classes = body.get("ClassSeriesFundList")
    if not isinstance(raw_classes, list) or len(raw_classes) == 0:
        return {
            "available": False,
            "fund_parent_id": body.get("FundParentID"),
            "fund_currency": body.get("FundCurrency"),
            "valuation_date": body.get("ValuationDate"),
            "net_asset_value": nav_native_early
            if nav_native_early is not None
            else _parse_money_value(body.get("NetAssetValue")),
            "net_asset_value_native": nav_native_early,
            "native_currency": nav_ccy_early,
            "classes": [],
        }

    classes_out: List[Dict[str, Any]] = []
    for entry in raw_classes:
        if not isinstance(entry, dict):
            continue
        class_id = (entry.get("FundID") or "").strip()
        if not class_id:
            continue
        class_code = (entry.get("ClassCode") or "").strip()
        fund_parent_id = (body.get("FundParentID") or "").strip()
        fund_base = FUND_NATIVE_CURRENCY.get(fund_parent_id, "CAD")
        navpu, class_ccy = _parse_class_navpu(entry, fund_base)
        ret_raw = _valuation_period_return(entry)
        display_class = _class_series_token(class_code, class_id) or class_code or class_id
        classes_out.append(
            {
                "class_id": class_id,
                "class_code": (entry.get("ClassCode") or "").strip() or None,
                "display_class": display_class,
                "navpu": navpu,
                "nav_currency": class_ccy,
                "nav_source": "diamond",
                "bps": _return_value_to_bps(ret_raw),
                "return_display": str(ret_raw).strip() if ret_raw is not None else None,
            }
        )

    fund_id = (body.get("FundParentID") or "").strip()
    header_nav, nav_ccy = pick_fund_net_asset_value(body, fund_id)
    root_nav = _parse_money_value(body.get("NetAssetValue"))
    class_sum = sum_class_net_assets_cad(body, fund_id)
    aum_closing, closing_label = pick_fund_aum_for_role(body, fund_id, "closing")
    aum_opening, opening_label = pick_fund_aum_for_role(body, fund_id, "opening")
    nav_native = aum_closing
    capital_flow, capital_flow_label = pick_capital_flow_adjustment(body)
    sheet_date = normalize_diamond_sheet_date(body.get("ValuationDate"))

    has_nav = any(c.get("navpu") is not None for c in classes_out)
    return {
        "available": has_nav and len(classes_out) > 0,
        "fund_parent_id": body.get("FundParentID"),
        "fund_currency": body.get("FundCurrency"),
        "valuation_date": body.get("ValuationDate"),
        "sheet_valuation_date": sheet_date,
        "net_asset_value": body.get("NetAssetValue"),
        "net_asset_value_native": nav_native,
        "fund_aum_closing": aum_closing,
        "fund_aum_closing_label": closing_label,
        "fund_aum_opening": aum_opening,
        "fund_aum_opening_label": opening_label,
        "native_currency": nav_ccy,
        "capital_flow": capital_flow,
        "capital_flow_label": capital_flow_label,
        "capital_flow_candidates": list_capital_flow_candidates(body),
        "aum_parse_version": 5,
        "aum_from_class_sum": class_sum is not None and nav_native == class_sum,
        "diamond_aum_components": {
            "root_net_asset_value": root_nav,
            "header_section_cad": header_nav,
            "class_sum_cad": class_sum,
            "closing_label": closing_label,
            "opening_label": opening_label,
        },
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


def _is_class_i_series(token: str) -> bool:
    """Match Class I series labels from Diamond (e.g. 500I, 550I, 200I)."""
    t = (token or "").strip().upper()
    if not t:
        return False
    if t in ("I", "CLASS I"):
        return True
    if re.match(r"^class\s*i\b", t, re.IGNORECASE):
        return True
    # EHP series codes: digits + I (500I), not USD series containing U (500UO).
    return bool(re.search(r"\dI$", t)) and "U" not in t


def pick_class_i_bps(classes: List[Dict[str, Any]]) -> Optional[int]:
    """Return valuation-period return (bps) for Class I share class."""
    for cls in classes or []:
        token = (
            cls.get("display_class")
            or cls.get("class_code")
            or cls.get("class_id")
            or ""
        )
        if _is_class_i_series(str(token)):
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
