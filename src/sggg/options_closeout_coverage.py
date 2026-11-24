"""PSC coverage contract for closeout controls; no network or startup side effects."""

import logging
import math
import time
from collections.abc import Callable
from datetime import date

logger = logging.getLogger(__name__)


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
            cursor.execute("SELECT DESCRIPTION, LONG_SHORT, QUANTITY, BBG_TICKER FROM psc_position_history "
                           "WHERE PORTFOLIO = ? AND POSN_DATE = ? "
                           "AND SECURITY_TYPE IN ('EquityOption','Equity Option')", (portfolio, snapshot))
            rows = cursor.fetchall()
            item["option_lines"] = len(rows)
            for row in rows:
                raw = str(row[0] or "").strip()
                bloomberg_ticker = str(row[3] or "").strip() if len(row) > 3 else ""
                security = canonicalize(bloomberg_ticker) or canonicalize(raw)
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
            logger.debug("Could not load PSC starting positions for %s", portfolio, exc_info=True)
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
    """Finish coverage and flag unidentified holdings on the returned report groups."""
    result = {**coverage, "issues": list(coverage.get("issues") or [])}
    for position in analysis.get("open_positions") or []:
        security = position.get("security")
        if not parse_contract(security):
            result["issues"].append(f"Open option contract could not be parsed: {security}")
            try:
                net = float(position.get("net_contracts") or 0)
            except (TypeError, ValueError):
                continue
            if not security or not math.isfinite(net) or abs(net) <= 1e-9:
                continue
            side = "Long" if net > 0 else "Short"
            holding = "contract remains" if abs(net) == 1 else "contracts remain"
            reason = (
                f"Unidentified option: {side.lower()} {abs(net):g} {holding} "
                f"(net {net:g}). Verify whether this position should be flat; "
                "expiry and exercise/assignment risk could not be determined."
            )
            groups = analysis.setdefault("groups", [])
            group = next((item for item in groups if item.get("security") == security), None)
            if group is None:
                group = {"group_id": security, "security": security,
                         "security_display": security, "net_end": net, "is_open": True,
                         "flags_count": 0, "trades_count": position.get("trades_count", 0)}
                groups.append(group)
            warnings = group.setdefault("position_warnings", [])
            if reason not in warnings:
                warnings.append(reason)
                group["flags_count"] = int(group.get("flags_count") or 0) + 1
    if exercise_status != "complete":
        result["issues"].append(f"Exercise coverage: {exercise_status}")
    result["issues"] = list(dict.fromkeys(result["issues"]))
    result["status"] = "incomplete" if result["issues"] else "complete"
    return result
