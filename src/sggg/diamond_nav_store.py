"""Persist Diamond GetNAVSheet summaries in Supabase for reuse across NAV checker runs."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from sggg.nav_sheet_parse import fund_aum_from_summary, normalize_valuation_date

logger = logging.getLogger(__name__)


def _parse_sheet_date(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw.isoformat()
    s = str(raw).strip()
    return s[:10] if len(s) >= 10 else None


_AUM_PARSE_VERSION = 8


def snapshot_usable(summary: dict[str, Any]) -> bool:
    """True when stored row is enough to skip a live GetNAVSheet call."""
    if int(summary.get("aum_parse_version") or 0) < _AUM_PARSE_VERSION:
        return False
    if summary.get("available"):
        return True
    return fund_aum_from_summary(summary) is not None


def load_snapshots_bulk(
    supabase: Any,
    fund_ids: list[str],
    valuation_dates: list[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Load cached summaries keyed by (fund_id, valuation_date yyyy-mm-dd).
    """
    out: dict[tuple[str, str], dict[str, Any]] = {}
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
        logger.debug("Could not load cached Diamond NAV snapshots", exc_info=True)
        return out
    return out


def upsert_snapshot(
    supabase: Any,
    fund_id: str,
    valuation_date: str,
    summary: dict[str, Any],
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
        "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    try:
        supabase.table("fund_admin_diamond_nav_snapshots").upsert(
            row,
            on_conflict="fund_id,valuation_date",
        ).execute()
    except Exception:
        logger.warning("Could not cache Diamond NAV snapshot for %s on %s", fund_id, vdate, exc_info=True)
