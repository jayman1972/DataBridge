"""PSC coverage contract for closeout controls; no network or startup side effects."""

from datetime import date
import math
import time
from typing import Callable


def load_start_positions(cursor, portfolios: list[str], report_date: str,
                         probe: Callable, canonicalize: Callable, normalize: Callable,
                         minimum_snapshot_date: str | None = None) -> tuple[dict, dict]:
    starting: dict[str, float] = {}
    per_security: dict[str, dict[str, float]] = {}
    coverage = {"status": "complete", "issues": [], "portfolios": [],
                "aggregation_scope": "security_net_across_funds",
                "opposing_portfolio_positions": []}
    report_day = date.fromisoformat(report_date)
    minimum_day = date.fromisoformat(minimum_snapshot_date) if minimum_snapshot_date else None
    coverage["minimum_snapshot_date"] = minimum_snapshot_date
    deadline = time.monotonic() + 15
    for portfolio in portfolios:
        item = {"portfolio": portfolio, "snapshot_date": None, "option_lines": 0, "status": "complete"}
        coverage["portfolios"].append(item)
        try:
            if time.monotonic() >= deadline:
                raise TimeoutError("PSC closeout coverage query budget exhausted")
            snapshot = probe(cursor, portfolio, report_date.replace("-", ""), lookback_days=14, deadline=deadline)
            if not snapshot:
                raise ValueError("No PSC snapshot strictly before report date")
            snapshot_day = date.fromisoformat(f"{snapshot[:4]}-{snapshot[4:6]}-{snapshot[6:8]}")
            item["snapshot_date"] = snapshot_day.isoformat()
            age = (report_day - snapshot_day).days
            item["age_calendar_days"] = age
            if age <= 0:
                raise ValueError("PSC snapshot is not prior to report date")
            if age > 7:
                item["status"] = "incomplete"
                coverage["issues"].append(f"{portfolio}: PSC snapshot is more than seven days old")
            if minimum_day and snapshot_day < minimum_day:
                item["status"] = "incomplete"
                coverage["issues"].append(f"{portfolio}: PSC snapshot predates required prior US session {minimum_snapshot_date}")
            if time.monotonic() >= deadline:
                raise TimeoutError("PSC closeout coverage query budget exhausted")
            cursor.execute("SELECT DESCRIPTION, LONG_SHORT, QUANTITY FROM psc_position_history "
                           "WHERE PORTFOLIO = ? AND POSN_DATE = ? "
                           "AND SECURITY_TYPE IN ('EquityOption','Equity Option')", (portfolio, snapshot))
            rows = cursor.fetchall()
            item["option_lines"] = len(rows)
            for row in rows:
                raw = str(row[0] or "").strip()
                security = canonicalize(raw)
                if not security:
                    security = normalize(raw) or raw.upper()
                    coverage["issues"].append(f"{portfolio}: option contract could not be parsed: {raw}")
                quantity = float(row[2] or 0)
                if not math.isfinite(quantity) or not security:
                    raise ValueError("Invalid PSC option identity or quantity")
                # PSC quantities are already signed; do not re-sign LONG_SHORT.
                starting[security] = starting.get(security, 0.0) + quantity
                by_portfolio = per_security.setdefault(security, {})
                by_portfolio[portfolio] = by_portfolio.get(portfolio, 0.0) + quantity
        except Exception as error:
            item["status"] = "incomplete"
            coverage["issues"].append(f"{portfolio}: {error}")
    for security, holdings in per_security.items():
        if any(q > 1e-9 for q in holdings.values()) and any(q < -1e-9 for q in holdings.values()):
            coverage["opposing_portfolio_positions"].append({"security": security, "portfolio_nets": holdings})
            coverage["issues"].append(f"{security}: opposing fund positions are netted; fund-level exercise coverage cannot be confirmed")
    if coverage["issues"]:
        coverage["status"] = "incomplete"
    return starting, coverage


def finish_coverage(coverage: dict, analysis: dict, parse_contract: Callable,
                    exercise_status: str) -> dict:
    result = {**coverage, "issues": list(coverage.get("issues") or [])}
    for position in analysis.get("open_positions") or []:
        if not parse_contract(position.get("security")):
            result["issues"].append(f"Open option contract could not be parsed: {position.get('security')}")
    if exercise_status != "complete":
        result["issues"].append(f"Exercise coverage: {exercise_status}")
    result["issues"] = list(dict.fromkeys(result["issues"]))
    result["status"] = "incomplete" if result["issues"] else "complete"
    return result
