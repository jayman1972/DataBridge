"""Persist Diamond GetNAVSheet summaries in Supabase for reuse across NAV checker runs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sggg.nav_sheet_parse import fund_aum_from_summary, normalize_valuation_date


def _parse_sheet_date(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw.isoformat()
    s = str(raw).strip()
    return s[:10] if len(s) >= 10 else None


_AUM_PARSE_VERSION = 4


def snapshot_usable(summary: Dict[str, Any]) -> bool:
    """True when stored row is enough to skip a live GetNAVSheet call."""
    if int(summary.get("aum_parse_version") or 0) < _AUM_PARSE_VERSION:
        return False
    if summary.get("available"):
        return True
    return fund_aum_from_summary(summary) is not None


def load_snapshots_bulk(
    supabase: Any,
    fund_ids: List[str],
    valuation_dates: List[str],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Load cached summaries keyed by (fund_id, valuation_date yyyy-mm-dd).
    """
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not supabase or not fund_ids or not valuation_dates:
        return out
    dates_norm = sorted({normalize_valuation_date(d) for d in valuation_dates})
    try:
        resp = (
            supabase.table("fund_admin_diamond_nav_snapshots")
            .select("fund_id, valuation_date, summary, available, fund_aum, fetched_at")
            .in_("fund_id", fund_ids)
            .in_("valuation_date", dates_norm)
            .execute()
        )
        for row in resp.data or []:
            fid = (row.get("fund_id") or "").strip()
            vdate = _parse_sheet_date(row.get("valuation_date"))
            summary = row.get("summary")
            if not fid or not vdate or not isinstance(summary, dict):
                continue
            if snapshot_usable(summary):
                out[(fid, vdate)] = summary
    except Exception:
        return out
    return out


def upsert_snapshot(
    supabase: Any,
    fund_id: str,
    valuation_date: str,
    summary: Dict[str, Any],
) -> None:
    if not supabase or not snapshot_usable(summary):
        return
    vdate = normalize_valuation_date(valuation_date)
    sheet_date = _parse_sheet_date(summary.get("valuation_date"))
    row = {
        "fund_id": fund_id,
        "valuation_date": vdate,
        "sheet_valuation_date": sheet_date,
        "available": bool(summary.get("available")),
        "fund_aum": fund_aum_from_summary(summary),
        "aum_currency": summary.get("native_currency"),
        "classes": summary.get("classes") or [],
        "summary": summary,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }
    try:
        supabase.table("fund_admin_diamond_nav_snapshots").upsert(
            row,
            on_conflict="fund_id,valuation_date",
        ).execute()
    except Exception:
        pass
