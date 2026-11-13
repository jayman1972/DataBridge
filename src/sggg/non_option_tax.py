"""Read-only, fund-admin access to cash-security and outright-futures fills."""

from datetime import date, timedelta

import requests
from flask import jsonify, request

from .options_tax_scope import OPTION_TRADE_SCOPE

# Cash/FX are transfers rather than security realizations. Bloomberg-identified
# futures options stay in the existing options feed even if PSC calls them Futures.
_OPTION_SCOPE = OPTION_TRADE_SCOPE.replace("BBG_TICKER", "COALESCE(BBG_TICKER, '')")
NON_OPTION_SCOPE = (
    "SECURITY_TYPE NOT IN ('Cash', 'FX', 'FXForward', 'Forward', 'Currency') "
    f"AND NOT {_OPTION_SCOPE}"
)
FILL_COLUMNS = (
    "ORDER_ID, SECURITY_SN, PORTFOLIO, STRATEGY, SECURITY, DESCRIPTION, "
    "SECURITY_TYPE, SEC_CCY, COUNTRY, BBG_TICKER, CUSIP, ISIN, SEDOL, "
    "TRADE_DATE_INT, TRADE_DATE_TIME, `ORDER` AS ORDER_ACTION, PRICE, ACT_QTTY, "
    "COMMISH_AMT, FEES_AMT, SEC_CCY_AMT, CONTRACT_SIZE, QUOTE_SIZE"
)
FILLS_SQL = (
    f"SELECT {FILL_COLUMNS} FROM psc_filled_orders "
    "WHERE PORTFOLIO = ? AND TRADE_DATE_INT <= ? "
    f"AND {NON_OPTION_SCOPE} "
    "AND SECURITY_SN IN (SELECT SECURITY_SN FROM psc_filled_orders "
    "WHERE PORTFOLIO = ? AND TRADE_DATE_INT BETWEEN ? AND ?) "
    "ORDER BY TRADE_DATE_INT, TRADE_DATE_TIME, ORDER_ID, STRATEGY, SECURITY"
)
FX_SQL = (
    "SELECT DISTINCT POSN_DATE, SEC_CCY, FX_SECURITY_TO_BASE "
    "FROM psc_position_history WHERE PORTFOLIO = ? AND POSN_DATE BETWEEN ? AND ? "
    "AND FX_SECURITY_TO_BASE > 0 ORDER BY POSN_DATE, SEC_CCY"
)
POSITIONS_SQL = (
    "SELECT POSN_DATE, PORTFOLIO, STRATEGY, SECURITY, SECURITY_TYPE, SEC_CCY, COUNTRY, "
    "LONG_SHORT, QUANTITY, AVG_PRICE, POSN_OPEN_DT, BBG_TICKER, DESCRIPTION "
    "FROM psc_position_history WHERE PORTFOLIO = ? "
    "AND POSN_DATE BETWEEN ? AND ? "
    "AND SECURITY_TYPE NOT IN ('Cash', 'FX', 'FXForward', 'Forward', 'Currency', 'EquityOption') "
    "ORDER BY POSN_DATE, STRATEGY, SECURITY"
)


def fetch_non_option_tax(conn, portfolio, start, end):
    """Retain opening fills, including before the tax year; never truncate basis."""
    cursor = conn.cursor()
    first, last = int(start.replace("-", "")), int(end.replace("-", ""))
    cursor.execute(FILLS_SQL, (portfolio, last, portfolio, first, last))
    columns = [col[0] for col in cursor.description]
    raw = cursor.fetchmany(100001)
    if len(raw) > 100000:
        raise ValueError("More than 100,000 security fills; narrow the portfolio before importing.")
    fills = [dict(zip(columns, row)) for row in raw]
    # Only retain securities traded during this report. Their complete earlier
    # fill history supplies cost and actual opening commissions, not a 90-day guess.
    active = {row["SECURITY_SN"] for row in fills if row["TRADE_DATE_INT"] >= first}
    fills = [row for row in fills if row["SECURITY_SN"] in active]
    cursor.execute(FX_SQL, (portfolio, first, last))
    columns = [col[0] for col in cursor.description]
    fx = [dict(zip(columns, row)) for row in cursor.fetchall()]
    checkpoint_start = int((date.fromisoformat(start) - timedelta(days=14)).strftime("%Y%m%d"))
    cursor.execute(POSITIONS_SQL, (portfolio, checkpoint_start, last))
    columns = [col[0] for col in cursor.description]
    positions = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return {"portfolio": portfolio, "start_date": start, "end_date": end,
            "non_option_trades": fills, "fx_history": fx, "position_checkpoints": positions,
            "history_complete": True}


def register_non_option_tax_routes(app, *, bridge):
    @app.post("/sggg/non-option-tax-reconciliation")
    def non_option_tax_reconciliation():
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        if not token:
            return jsonify({"error": "Sign in to Fund Admin before importing trades."}), 401
        try:
            auth = requests.get(
                f"{bridge.SUPABASE_URL.rstrip('/')}/auth/v1/user",
                headers={"apikey": bridge.SUPABASE_KEY, "Authorization": f"Bearer {token}"},
                timeout=10,
            )
            user_id = auth.json().get("id") if auth.status_code == 200 else None
            allowed = user_id and any(bridge.supabase.rpc(
                "has_role", {"_user_id": user_id, "_role": role}
            ).execute().data is True for role in ("admin", "sggg"))
            if not allowed:
                return jsonify({"error": "Fund Admin access is required."}), 403
        except (requests.RequestException, ValueError):
            return jsonify({"error": "Could not verify your Fund Admin session."}), 503
        payload = request.get_json(silent=True) or {}
        try:
            start = date.fromisoformat(str(payload.get("start_date", "")))
            end = date.fromisoformat(str(payload.get("end_date", "")))
            if start > end or (end - start).days > 1827:
                raise ValueError("Choose a report period of five years or less.")
            portfolio = bridge._psc_portfolio_from_request(payload)
            if not portfolio:
                raise ValueError("A mapped fund is required.")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        pyodbc, error = bridge._pyodbc_or_503()
        if error:
            return error
        conn = None
        try:
            conn = pyodbc.connect("DSN=PSC_VIEWER", timeout=20)
            conn.timeout = 90
            return bridge._json_response(bridge._log_serialize(
                fetch_non_option_tax(conn, portfolio, start.isoformat(), end.isoformat())
            ), 200)
        except Exception:
            app.logger.exception("Non-option tax import failed")
            return jsonify({"error": "AlphaDesk security import failed; see the bridge log for details."}), 500
        finally:
            if conn is not None:
                conn.close()
