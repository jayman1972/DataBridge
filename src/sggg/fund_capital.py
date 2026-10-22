"""Date-aligned PSC fund capital, distinct from lagged position-level transfers."""

from __future__ import annotations

import math
import secrets
from datetime import date, timedelta
from typing import Any

from flask import jsonify, request

CAPITAL_BASIS_VERSION = 1
SOURCE = "psc_hist_group_portfolio_nav"
FUNDS = {
    "EHP Alpha",
    "EHP Select Alt",
    "EHP Strat Inc Alt",
    "EHP Tact Growth Alt",
    "The Soar Fund",
}


def capital_date(value: str) -> str:
    compact = str(value).replace("-", "")
    if len(compact) != 8 or not compact.isdigit():
        raise ValueError("A capital date in YYYY-MM-DD or YYYYMMDD is required")
    return date.fromisoformat(f"{compact[:4]}-{compact[4:6]}-{compact[6:]}").isoformat()


def read_fund_capital(cursor: Any, fund: str, requested_date: str) -> dict[str, Any]:
    if fund not in FUNDS:
        raise ValueError("Unsupported production fund")
    nav_date = capital_date(requested_date)
    identities = set()
    for table in ("psc_all_targets", "psc_all_transactions"):
        cursor.execute(
            f"SELECT DISTINCT PORTFOLIO_ID FROM {table} WHERE PORTFOLIO=?", (fund,)
        )
        identities = {int(row[0]) for row in cursor.fetchall() if row[0] is not None}
        if identities:
            break
    if len(identities) != 1:
        raise ValueError("PSC fund identity is missing or ambiguous; sizing is blocked")
    cursor.execute(
        "SELECT NAV,Transfers_In FROM psc_hist_group_portfolio_nav "
        "WHERE Portfolio_ID=? AND date_int=?",
        (identities.pop(), int(nav_date.replace("-", ""))),
    )
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] is None or rows[0][1] is None:
        raise ValueError(
            f"Verified fund NAV and transfers are unavailable for {nav_date}"
        )
    nav, transfers = float(rows[0][0]), float(rows[0][1])
    after = nav + transfers
    if (
        not all(math.isfinite(value) for value in (nav, transfers, after))
        or min(nav, after) <= 0
    ):
        raise ValueError("PSC post-transfer fund capital is invalid; sizing is blocked")
    return {
        "basis_version": CAPITAL_BASIS_VERSION,
        "source": SOURCE,
        "fund": fund,
        "nav_date": nav_date,
        "nav_before_flows": nav,
        "net_transfers": transfers,
        "nav_after_flows": after,
    }


def fetch_fund_capital(fund: str, requested_date: str) -> dict[str, Any]:
    import pyodbc

    connection = pyodbc.connect("DSN=PSC_VIEWER", timeout=15)
    try:
        return read_fund_capital(connection.cursor(), fund, requested_date)
    finally:
        connection.close()


def apply_capital_basis(
    payload: dict[str, Any], capital: dict[str, Any]
) -> dict[str, Any]:
    raw_nav = payload.get("fund_nav")
    if (
        payload.get("nav_date") != capital["nav_date"]
        or raw_nav is None
        or not math.isfinite(float(raw_nav))
        or abs(float(raw_nav) - capital["nav_before_flows"]) > 0.01
    ):
        raise ValueError("PSC positions and fund capital disagree; sizing is blocked")
    nav = capital["nav_after_flows"]
    for position in payload.get("positions", []):
        exposure = position.get("exposure")
        position["exposure_pct_nav"] = (
            float(exposure) / nav * 100 if exposure is not None else None
        )
    return {**payload, "fund_nav": nav, "capital_basis": capital}


def _prior_nav_date(client: Any, trade_date: str) -> str:
    floor = (date.fromisoformat(trade_date) - timedelta(days=14)).isoformat()
    sessions = (
        client.table("bt_exchange_sessions")
        .select("session_date,is_open")
        .eq("exchange_id", "XTSE")
        .gte("session_date", floor)
        .lt("session_date", trade_date)
        .execute()
        .data
    )
    overrides = (
        client.table("bt_exchange_calendar_overrides")
        .select("session_date,force_open,force_closed")
        .eq("exchange_id", "XTSE")
        .gte("session_date", floor)
        .lt("session_date", trade_date)
        .execute()
        .data
    )
    for offset in range(1, 15):
        candidate = (
            date.fromisoformat(trade_date) - timedelta(days=offset)
        ).isoformat()
        override = next(
            (row for row in overrides if row["session_date"] == candidate), {}
        )
        session = next(
            (row for row in sessions if row["session_date"] == candidate), None
        )
        if override.get("force_open"):
            return candidate
        if override.get("force_closed"):
            continue
        if session is None:
            raise ValueError("Fund calendar is incomplete; export is blocked")
        if session["is_open"]:
            return candidate
    raise ValueError("Prior fund business day cannot be verified")


def verify_export_capital(
    client: Any, user_id: str, filename: str, digest: str
) -> None:
    """Recheck the persisted sizing audit, not proof supplied by a browser."""
    batches = (
        client.table("bt_sggg_trade_batches")
        .select("source_draft_ids,trade_date")
        .eq("created_by", user_id)
        .eq("filename", filename)
        .eq("payload_sha256", digest)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if batches:
        batch = batches[0]
        ids = batch["source_draft_ids"]
        audits = (
            client.table("bt_sggg_trade_drafts")
            .select("dashboard_fund_name,trade_date,configuration_snapshot")
            .in_("id", ids)
            .eq("user_id", user_id)
            .eq("status", "ready")
            .eq("trade_date", batch["trade_date"])
            .execute()
            .data
        )
        if len(audits) != len(set(ids)):
            raise ValueError("Trade drafts changed; rerun and review before export")
    else:
        audits = (
            client.table("bt_sggg_trade_exports")
            .select("dashboard_fund_name,trade_date,configuration_snapshot")
            .eq("exported_by", user_id)
            .eq("filename", filename)
            .eq("payload_sha256", digest)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
    if not audits:
        raise ValueError("A verified trade sizing audit is required before export")
    # One fresh snapshot per fund/date in this request, never across exports.
    expected_by_date: dict[str, str] = {}
    capital_by_fund_date: dict[tuple[str, str], dict[str, Any]] = {}
    adjustment_by_fund_date: dict[tuple[str, str], float] = {}
    for audit in audits:
        fund = audit["dashboard_fund_name"]
        trade_date = capital_date(audit["trade_date"])
        if trade_date not in expected_by_date:
            expected_by_date[trade_date] = _prior_nav_date(client, trade_date)
        expected = expected_by_date[trade_date]
        capital_key = ("The Soar Fund" if fund == "SOAR Fund" else fund, expected)
        if capital_key not in capital_by_fund_date:
            capital_by_fund_date[capital_key] = fetch_fund_capital(*capital_key)
        current = capital_by_fund_date[capital_key]
        saved = audit.get("configuration_snapshot") or {}
        basis = saved.get("fundCapitalBasis")
        if not isinstance(basis, dict) or any(
            basis.get(key) != value for key, value in current.items()
        ):
            raise ValueError(
                "NAV/transfer verification is missing or changed; rerun before export"
            )
        adjustment_key = (fund, trade_date)
        if adjustment_key not in adjustment_by_fund_date:
            adjustments = (
                client.table("bt_production_fund_rebalance_inputs")
                .select("nav_adjustment_cad")
                .eq("fund_name", fund)
                .eq("trade_date", trade_date)
                .execute()
                .data
            )
            adjustment_by_fund_date[adjustment_key] = (
                float(adjustments[0]["nav_adjustment_cad"]) if adjustments else 0
            )
        adjustment = adjustment_by_fund_date[adjustment_key]
        expected_values = {
            "sourceNavCad": current["nav_after_flows"],
            "navAdjustmentCad": adjustment,
            "adjustedNavCad": current["nav_after_flows"] + adjustment,
        }
        if expected_values["adjustedNavCad"] <= 0 or any(
            not isinstance(saved.get(key), (int, float))
            or not math.isfinite(saved[key])
            or abs(saved[key] - value) > 0.01
            for key, value in expected_values.items()
        ):
            raise ValueError(
                "Fund sizing or cash adjustment changed; rerun before export"
            )


def register_fund_capital_routes(app: Any, *, service_key: str) -> None:
    @app.route("/sggg/fund-capital", methods=["GET"])
    def fund_capital():
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if (
            not token
            or not service_key
            or not secrets.compare_digest(token, service_key)
        ):
            return jsonify({"error": "Service authorization is required"}), 401
        try:
            capital = fetch_fund_capital(
                request.args.get("fund", ""), request.args.get("date", "")
            )
            return jsonify(
                {
                    "fund_nav": capital["nav_after_flows"],
                    "nav_date": capital["nav_date"],
                    "capital_basis": capital,
                    "positions": [],
                }
            )
        except ValueError as error:
            return jsonify({"error": str(error)}), 422
        except Exception:
            app.logger.exception("Fund capital source failed")
            return jsonify(
                {"error": "Fund capital source unavailable; sizing is blocked"}
            ), 503

    @app.after_request
    def production_capital_basis(response: Any):
        # Enrich only the dedicated production profile. General portfolio display
        # and reconciliation retain the vendor's original pre-transfer NAV.
        body = request.get_json(silent=True) if request.is_json else {}
        profile = request.args.get("profile") or (
            body.get("profile") if isinstance(body, dict) else None
        )
        if (
            request.path != "/sggg/portfolio"
            or response.status_code != 200
            or profile != "production_snapshot"
        ):
            return response
        try:
            payload = response.get_json()
            capital = fetch_fund_capital(payload["fund"], payload["nav_date"])
            response.set_data(app.json.dumps(apply_capital_basis(payload, capital)))
        except Exception:
            app.logger.exception("Production capital verification failed")
            response.set_data(
                app.json.dumps(
                    {
                        "error": "Verified NAV and transfers are required; sizing is blocked"
                    }
                )
            )
            response.status_code = 422
        return response
