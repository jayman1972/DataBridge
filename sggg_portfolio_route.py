"""
Add /sggg/portfolio to your Data Bridge (e.g. wealth-scope-ui).

Option A - Register as blueprint (copy this file into your bridge project):
  from sggg_portfolio_route import sggg_bp
  app.register_blueprint(sggg_bp)

Option B - Copy the sggg_portfolio function and this route into your main app:
  @app.route("/sggg/portfolio", methods=["GET", "POST"])
  def sggg_portfolio():
      ... (paste the body of sggg_portfolio from this file)

Requires: OpenVPN connected, pyodbc, ODBC DSN=PSC_VIEWER.
"""

import os
import traceback
from datetime import datetime
from flask import Blueprint, request, jsonify

sggg_bp = Blueprint("sggg", __name__)


@sggg_bp.route("/sggg/portfolio", methods=["GET", "POST"])
def sggg_portfolio():
    """
    Fetch Fund NAV and full position report from SGGG (PSC).
    Called by Supabase Edge Function refresh-portfolio via tunnel.
    """
    try:
        data = request.get_json(silent=True) or {}
        query_date = request.args.get("date") or data.get("date")
        if query_date:
            query_date = str(query_date).replace("-", "").replace("/", "")[:8]
        else:
            query_date = datetime.now().strftime("%Y%m%d")

        try:
            import pyodbc
        except ImportError:
            return jsonify({
                "error": "pyodbc not installed. pip install pyodbc and configure ODBC DSN=PSC_VIEWER.",
                "fund_nav": None,
                "nav_date": None,
                "positions": [],
            }), 503

        conn = None
        try:
            conn = pyodbc.connect("DSN=PSC_VIEWER")
            cursor = conn.cursor()
            sql = (
                "SELECT STRATEGY, TRADE_GROUP, COMPANY_SYMBOL, DESCRIPTION, SECURITY_TYPE, "
                "SEC_CCY AS Currency, BBG_TICKER, SECTOR, COUNTRY, LONG_SHORT, QUANTITY, "
                "AVG_PRICE, CLOSE_PRICE, PRICE_PROFIT, INTEREST, DIVIDENDS, VALUE, EXPOSURE, "
                "DAY_PROFIT, PORTFOLIO_NAV "
                "FROM psc_position_history "
                "WHERE PORTFOLIO = 'EHP Select Alt' AND POSN_DATE = ? "
                "AND SECURITY_TYPE IN ('Stock', 'EquityOption', 'LeveragedETF', 'Futures') "
                "AND STRATEGY <> 'Risk Arbitrage' "
                "ORDER BY STRATEGY, TRADE_GROUP, COMPANY_SYMBOL"
            )
            cursor.execute(sql, (query_date,))
            rows = cursor.fetchall()
            fund_nav = None
            if rows and rows[0] and len(rows[0]) > 19 and rows[0][19] is not None:
                try:
                    fund_nav = float(rows[0][19])
                except (TypeError, ValueError, IndexError):
                    pass
            nav_date_iso = (
                f"{query_date[:4]}-{query_date[4:6]}-{query_date[6:8]}"
                if len(query_date) == 8
                else query_date
            )

            usd_cad_rate = None
            try:
                env_rate = os.environ.get("USD_CAD_RATE")
                if env_rate:
                    usd_cad_rate = float(env_rate)
            except (TypeError, ValueError):
                pass

            def _num(v):
                if v is None:
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            def _str(v):
                return (v or "").strip() or None

            positions = []
            for row in rows:
                exposure = _num(row[17]) if len(row) > 17 else None
                pct_nav = (
                    (exposure / fund_nav * 100)
                    if (fund_nav and fund_nav != 0 and exposure is not None)
                    else None
                )
                sec_type = _str(row[4])
                bbg = _str(row[6])
                company_sym = _str(row[2])
                # Options always use Bloomberg for quotes; if BBG_TICKER is null, use company_symbol
                if (sec_type or "").upper() in ("EQUITYOPTION", "OPTION") and not bbg and company_sym:
                    bbg = company_sym
                positions.append({
                    "strategy": _str(row[0]),
                    "trade_group": _str(row[1]),
                    "company_symbol": company_sym,
                    "description": _str(row[3]),
                    "security_type": sec_type,
                    "currency": _str(row[5]),
                    "bbg_ticker": bbg or _str(row[6]),
                    "sector": _str(row[7]),
                    "country": _str(row[8]),
                    "long_short": _str(row[9]),
                    "quantity": _num(row[10]) if len(row) > 10 else None,
                    "avg_price": _num(row[11]) if len(row) > 11 else None,
                    "close_price": _num(row[12]) if len(row) > 12 else None,
                    "price_profit": _num(row[13]) if len(row) > 13 else None,
                    "interest": _num(row[14]) if len(row) > 14 else None,
                    "dividends": _num(row[15]) if len(row) > 15 else None,
                    "value": _num(row[16]) if len(row) > 16 else None,
                    "exposure": exposure,
                    "exposure_pct_nav": pct_nav,
                    "day_profit": _num(row[18]) if len(row) > 18 else None,
                    "profit": None,
                })
        finally:
            if conn:
                conn.close()

        return jsonify({
            "fund_nav": fund_nav,
            "nav_date": nav_date_iso,
            "usd_cad_rate": usd_cad_rate,
            "positions": positions,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "fund_nav": None,
            "nav_date": None,
            "usd_cad_rate": None,
            "positions": [],
        }), 500
