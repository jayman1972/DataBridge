"""Pure scheduling and payload helpers for Options Closeout alerts."""

from __future__ import annotations

from datetime import datetime, time
from typing import Any


def should_run_closeout_alert(
    eastern_now: datetime,
    last_completed_date: str | None,
    enabled: bool = True,
) -> bool:
    """True once per weekday between 3:45 p.m. and 4:00 p.m. Eastern."""
    if not enabled or eastern_now.weekday() >= 5:
        return False
    report_date = eastern_now.date().isoformat()
    if last_completed_date == report_date:
        return False
    current_time = eastern_now.time().replace(tzinfo=None)
    return time(15, 45) <= current_time < time(16, 0)


def collect_flagged_groups(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a compact, deduplicated notification payload from report output."""
    reasons_by_security: dict[str, list[str]] = {}
    for trade in report.get("trades") or []:
        security = str(trade.get("security") or "").strip()
        reason = str(trade.get("flag_reason") or "").strip()
        if security and reason:
            reasons_by_security.setdefault(security, []).append(reason)

    flagged: list[dict[str, Any]] = []
    for group in report.get("groups") or []:
        if int(group.get("flags_count") or 0) <= 0:
            continue
        security = str(group.get("security") or "").strip()
        reasons = list(reasons_by_security.get(security) or [])
        exercise_reason = str(
            (group.get("auto_exercise_risk") or {}).get("flag_reason") or ""
        ).strip()
        if exercise_reason:
            reasons.append(exercise_reason)
        unique_reasons = list(dict.fromkeys(reason for reason in reasons if reason))
        if not unique_reasons:
            unique_reasons = ["Flagged by Options Closeout Report controls"]
        flagged.append(
            {
                "security": security,
                "security_display": group.get("security_display") or security,
                "net_end": group.get("net_end"),
                "reasons": unique_reasons,
            }
        )
    return flagged
