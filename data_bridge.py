#!/usr/bin/env python3
"""
Data Bridge Service
Bloomberg data fetching service using BLPAPI (Bloomberg Terminal API).

Note: BQL is only available within BQuant IDE, not in external IDEs like Cursor.
Therefore, this service uses BLPAPI which works in any Python environment.
"""

import os
import sys
import json
import traceback
import time
from datetime import datetime, timedelta, timezone, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Any
from flask import Flask, request, jsonify
from flask_cors import CORS

# Try to import zoneinfo for timezone handling (Python 3.9+)
try:
    from zoneinfo import ZoneInfo
    HAS_ZONEINFO = True
    HAS_PYTZ = False
except ImportError:
    # Fallback for Python < 3.9: use pytz if available
    try:
        import pytz
        HAS_ZONEINFO = False
        HAS_PYTZ = True
    except ImportError:
        # No timezone support - will use naive datetime (not ideal)
        HAS_ZONEINFO = False
        HAS_PYTZ = False

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from bloomberg import get_bloomberg_client, BloombergClientType
from sggg.diamond_client import get_diamond_client

from supabase import create_client, Client

# Configuration
# Load from environment variables (required)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Load from config file if environment variables not set
if not SUPABASE_URL or not SUPABASE_KEY:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _projects = os.path.normpath(os.path.join(_script_dir, ".."))
    config_locations = [
        os.path.join(_script_dir, "bloomberg-service.env"),
        os.path.join(_projects, "market-dashboard", "bloomberg-service.env"),
        os.path.join(_projects, "wealth-scope-ui", "bloomberg-service.env"),
    ]
    config_file = None
    for p in config_locations:
        if os.path.exists(p):
            config_file = p
            break
    if config_file:
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key == "SUPABASE_URL":
                            SUPABASE_URL = value
                        elif key == "SUPABASE_SERVICE_ROLE_KEY":
                            SUPABASE_KEY = value
                        elif key == "SGGG_DIAMOND_USERNAME":
                            os.environ["SGGG_DIAMOND_USERNAME"] = value
                        elif key == "SGGG_DIAMOND_PASSWORD":
                            os.environ["SGGG_DIAMOND_PASSWORD"] = value
                        elif key == "SGGG_DIAMOND_FUND_ID":
                            os.environ["SGGG_DIAMOND_FUND_ID"] = value
        except Exception as e:
            print(f"Error reading config file {config_file}: {e}")

# Load SGGG Diamond API config (optional) from same config file
for _config_dir in [os.path.dirname(os.path.abspath(__file__)), os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "market-dashboard"))]:
    _cfg = os.path.join(_config_dir, "bloomberg-service.env")
    if os.path.exists(_cfg):
        try:
            with open(_cfg, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        k, v = k.strip(), v.strip()
                        if k in ("SGGG_DIAMOND_USERNAME", "SGGG_DIAMOND_PASSWORD", "SGGG_DIAMOND_FUND_ID") and v:
                            os.environ[k] = v
        except Exception as e:
            print(f"Error reading SGGG config from {_cfg}: {e}")
        break

# Uses BLPAPI (BQL only available in BQuant IDE)
SERVICE_PORT = int(os.getenv("PORT", "5000"))

# Clarifi/EHP directory (for /clarifi/process and /ehp/process)
USERNAME = os.getenv("USERNAME") or os.getenv("USER") or "user"
CLARIFI_DIR = os.getenv("CLARIFI_DIR", f"C:\\Users\\{USERNAME}\\OneDrive\\Desktop\\EHP_Files\\DailyExports from Clarifi\\")

# Bloomberg field mappings (matches existing mappings)
BLOOMBERG_MAPPINGS = [
    {"databaseColumn": "gdp_nowcast_ny_fed", "ticker": "NOWCYQCP Index", "field": "PX_LAST"},
    {"databaseColumn": "gdp_nowcast_atlanta_fed", "ticker": "GDGCAFJP Index", "field": "PX_LAST"},
    {"databaseColumn": "gdp_nowcast_bloomberg", "ticker": "BENWUSGC Index", "field": "PX_LAST"},
    {"databaseColumn": "cpi_truflation", "ticker": "TRUFUSYY Index", "field": "PX_LAST"},
    {"databaseColumn": "cpi_truflation_core", "ticker": "TRUFUSCZ Index", "field": "PX_LAST"},
    {"databaseColumn": "cpi_cleveland_fed", "ticker": "CLEVCPYC Index", "field": "PX_LAST"},
    {"databaseColumn": "cpi_core_cleveland_fed", "ticker": "CLEVXCYC Index", "field": "PX_LAST"},
    {"databaseColumn": "spx_index_close_price", "ticker": "SPX Index", "field": "PX_LAST"},
    {"databaseColumn": "spx_pct_members_14d_rsi_above_70", "ticker": "SPX Index", "field": "PCT_MEMB_WITH_14D_RSI_GT_70"},
    {"databaseColumn": "spx_pct_members_above_upper_bollinger", "ticker": "SPX Index", "field": "PCT_MEMB_PX_ABV_UPPER_BOLL_BAND"},
    {"databaseColumn": "spx_up_vs_down_volume", "ticker": ".UPVSDOWN U Index", "field": "PX_LAST"},
    {"databaseColumn": "spx_pct_members_new_52w_high", "ticker": "SPX Index", "field": "PCT_MEMBERS_WITH_NEW_52W_HIGHS"},
    {"databaseColumn": "spx_30d_rsi", "ticker": "SPX Index", "field": "RSI 30D"},
    {"databaseColumn": "spx_rsi_14d", "ticker": "SPX Index", "field": "RSI 14D"},
    {"databaseColumn": "vix_25_delta_call_to_put_ratio", "ticker": ".25DVIX U Index", "field": "PX_LAST"},
    {"databaseColumn": "spx_pct_members_below_lower_bollinger", "ticker": "SPX Index", "field": "PCT_MEMB_PX_BLW_LWR_BOLL_BAND"},
    {"databaseColumn": "spx_pct_members_above_50d_ma", "ticker": "SPX Index", "field": "PCT_MEMB_PX_GT_50D_MOV_AVG"},
    {"databaseColumn": "spx_pct_members_above_10d_ma", "ticker": "SPX Index", "field": "PCT_MEMB_PX_GT_10D_MOV_AVG"},
    {"databaseColumn": "spx_pct_members_14d_rsi_below_30", "ticker": "SPX Index", "field": "PCT_MEMB_WITH_14D_RSI_LT_30"},
    {"databaseColumn": "nyse_new_highs_vs_new_lows", "ticker": "NWHLSENY Index", "field": "PX_LAST"},
    {"databaseColumn": "vix_1_2_month_spread", "ticker": ".VIX1-2 Index", "field": "PX_LAST"},
    {"databaseColumn": "cboe_implied_1m_correlation", "ticker": "COR1M Index", "field": "PX_LAST"},
    {"databaseColumn": "redbook_same_store_sales_yoy", "ticker": "REDSWYOY Index", "field": "PX_LAST"},
    {"databaseColumn": "asa_temp_staffing_index_yoy", "ticker": "ASA INDX Index", "field": "PX_LAST"},
    {"databaseColumn": "vix_index_close_price", "ticker": "VIX Index", "field": "PX_LAST"},
    {"databaseColumn": "spx_put_call_ratio", "ticker": "PCUSEQTR Index", "field": "PX_LAST"},
]

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}) 

@app.after_request
def add_cors_headers(response):
    response.headers.add("Access-Control-Allow-Private-Network", "true")
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response

# Initialize Supabase client
supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print(f"[OK] Connected to Supabase: {SUPABASE_URL}")
    except Exception as e:
        print(f"[FAIL] Failed to connect to Supabase: {e}")
        sys.exit(1)
else:
    print("[FAIL] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables required")
    sys.exit(1)

# Initialize BLPAPI client (BQL only available in BQuant IDE)
bloomberg_client = None
try:
    bloomberg_client = get_bloomberg_client()
    client_type_name = type(bloomberg_client).__name__
    print(f"[OK] Initialized Bloomberg client: {client_type_name}")
    
    # Test availability
    if bloomberg_client.is_available():
        print(f"[OK] Bloomberg client is available and connected")
    else:
        print(f"[WARN] Bloomberg client initialized but connection test failed")
        print(f"  Ensure Bloomberg Terminal is running and you are logged in")
except Exception as e:
    print(f"[FAIL] Failed to initialize Bloomberg client: {e}")
    print(f"\nEnsure:")
    print(f"1. Bloomberg Terminal is running")
    print(f"2. You are logged in to Bloomberg Terminal")
    print(f"3. blpapi package is installed: pip install blpapi")
    print(f"\nError details:")
    print(f"{traceback.format_exc()}")
    sys.exit(1)


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    if not bloomberg_client:
        return jsonify({
            "status": "error",
            "service": "data-bridge",
            "error": "Bloomberg client not initialized"
        }), 500
    
    is_available = bloomberg_client.is_available()
    client_info = {
        "client_type": type(bloomberg_client).__name__,
        "available": is_available
    }
    
    status_code = 200 if is_available else 503
    return jsonify({
        "status": "ok" if is_available else "unavailable",
        "service": "data-bridge",
        "bloomberg": client_info
    }), status_code


def _is_us_market_hours() -> bool:
    """True if current time is 9:30am-4:00pm Eastern (US market hours)."""
    try:
        if HAS_ZONEINFO:
            eastern = ZoneInfo("America/New_York")
        elif HAS_PYTZ:
            eastern = pytz.timezone("America/New_York")
        else:
            return False
        now = datetime.now(eastern).time()
        return dt_time(9, 30) <= now <= dt_time(16, 0)
    except Exception:
        return False


@app.route("/bloomberg/quotes", methods=["POST"])
def bloomberg_quotes():
    """
    Fetch price for given Bloomberg tickers (options etc).
    During US market hours (9:30am-4:00pm EST): uses PX_MID when available.
    Outside market hours: uses PX_OFFICIAL_CLOSE when available.
    Fallback: PX_LAST.
    Body: { "tickers": ["TICKER1 Equity", "TICKER2 Equity"] }
    Returns: { "TICKER1 Equity": 1.23, "TICKER2 Equity": 4.56 }
    """
    try:
        data = request.get_json() or {}
        tickers = data.get("tickers") or []
        if not isinstance(tickers, list):
            tickers = []
        tickers = [str(t).strip() for t in tickers if t]

        # Explicit logging for portfolio options debugging
        print("[bloomberg/quotes] Request received. Tickers:", tickers)
        if not tickers:
            print("[bloomberg/quotes] No tickers - returning empty")
            return jsonify({}), 200

        in_market_hours = _is_us_market_hours()
        fields = ["PX_LAST", "PX_MID", "PX_OFFICIAL_CLOSE"]
        print("[bloomberg/quotes] US market hours:", in_market_hours, "| Sending to Bloomberg - tickers:", tickers, "fields:", fields)
        reference_data = bloomberg_client.get_reference_data(
            tickers=tickers,
            fields=fields
        )
        print("[bloomberg/quotes] Bloomberg response keys:", list(reference_data.keys()) if reference_data else [])

        result = {}
        for ticker, data_row in reference_data.items():
            if not isinstance(data_row, dict) or "error" in data_row:
                if isinstance(data_row, dict) and "error" in data_row:
                    print(f"[bloomberg/quotes] Bloomberg error for {ticker}: {data_row.get('error')}")
                continue

            val = None
            if in_market_hours:
                val = data_row.get("PX_MID")
            if val is None:
                val = data_row.get("PX_OFFICIAL_CLOSE") if not in_market_hours else None
            if val is None:
                val = data_row.get("PX_LAST")

            if val is not None:
                try:
                    result[ticker] = float(val)
                except (ValueError, TypeError):
                    pass

        print("[bloomberg/quotes] Returning", len(result), "prices (mode:", "mid" if in_market_hours else "official_close", "):", dict(result))
        return jsonify(result), 200
    except Exception as e:
        print("[bloomberg/quotes] Exception:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/bloomberg-update", methods=["POST"])
def bloomberg_update():
    """
    Main endpoint called by Edge Function.
    Fetches Bloomberg data and writes to Supabase.
    """
    try:
        data = request.get_json() or {}
        from_date = data.get("fromDate")
        to_date = data.get("toDate")
        
        # Default to today if not specified
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            # Default to 1983-01-01 if no from_date
            from_date = "1983-01-01"

        print(f"Bloomberg update requested: {from_date} to {to_date}")
        print(f"Using client: {type(bloomberg_client).__name__}")

        # Group mappings by ticker
        ticker_groups = {}
        for mapping in BLOOMBERG_MAPPINGS:
            ticker = mapping["ticker"]
            if ticker not in ticker_groups:
                ticker_groups[ticker] = []
            ticker_groups[ticker].append({
                "field": mapping["field"],
                "dbColumn": mapping["databaseColumn"]
            })

        # Fetch data for each ticker
        all_records = {}  # date -> {dbColumn: value}
        errors = []

        for ticker, field_mappings in ticker_groups.items():
            try:
                fields = list(set([m["field"] for m in field_mappings]))
                print(f"Fetching {ticker} with fields: {fields}")

                # Use the unified client interface
                bloomberg_data = bloomberg_client.get_historical_data(
                    ticker=ticker,
                    fields=fields,
                    start_date=from_date,
                    end_date=to_date
                )

                # Map Bloomberg data to database records
                for record in bloomberg_data:
                    date = record.get("date")
                    if not date:
                        continue

                    if date not in all_records:
                        all_records[date] = {"date": date}

                    # Map each field to database column
                    for field_mapping in field_mappings:
                        field = field_mapping["field"]
                        db_column = field_mapping["dbColumn"]
                        
                        if field in record:
                            value = record[field]
                            if value is not None:
                                # Convert to float if possible
                                try:
                                    all_records[date][db_column] = float(value)
                                except (ValueError, TypeError):
                                    pass

                print(f"  [OK] {ticker}: {len(bloomberg_data)} records")

            except Exception as e:
                error_msg = f"Error fetching {ticker}: {str(e)}"
                print(f"  [FAIL] {error_msg}")
                errors.append(error_msg)
                traceback.print_exc()

        # Write to Supabase
        if all_records:
            records_list = list(all_records.values())
            print(f"Writing {len(records_list)} records to Supabase...")

            try:
                result = supabase.table("market_data").upsert(
                    records_list,
                    on_conflict="date"
                ).execute()

                print(f"[OK] Successfully wrote {len(records_list)} records")
                return jsonify({
                    "success": True,
                    "inserted": len(records_list),
                    "errors": errors,
                    "client_type": type(bloomberg_client).__name__
                })

            except Exception as e:
                error_msg = f"Error writing to Supabase: {str(e)}"
                print(f"[FAIL] {error_msg}")
                errors.append(error_msg)
                return jsonify({
                    "success": False,
                    "inserted": 0,
                    "errors": errors
                }), 500
        else:
            return jsonify({
                "success": True,
                "inserted": 0,
                "errors": errors,
                "message": "No records to insert"
            })

    except Exception as e:
        error_msg = f"Bloomberg update error: {str(e)}"
        print(f"[FAIL] {error_msg}")
        traceback.print_exc()
        return jsonify({
            "success": False,
            "inserted": 0,
            "errors": [error_msg]
        }), 500


# ---------------------------------------------------------------------------
# /historical and /reference - used by market-dashboard update Edge Function
# ---------------------------------------------------------------------------
def _normalize_bloomberg_ticker(ticker: str) -> str:
    """Bloomberg API expects 'Equity' not 'EQUITY' for equity securities."""
    if not ticker:
        return ticker
    t = ticker.strip()
    if t.endswith(" EQUITY"):
        return t[:-7] + " Equity"
    return t


def _get_canadian_ticker_variants(ticker: str) -> list:
    """Return alternate Bloomberg ticker formats to try for Canadian securities."""
    t = ticker.strip()
    variants = [t]
    # "QBTL CN Equity" -> ["QBTL CN Equity", "QBTL:CN", "QBTL:CT"]
    if " CN " in t and ("Equity" in t or "EQUITY" in t.upper()):
        base = t.split(" CN ")[0].strip()
        variants.append(f"{base}:CN")
        variants.append(f"{base}:CT")
    return variants


@app.route("/historical-debug", methods=["POST"])
def historical_debug():
    """
    Debug endpoint: same as /historical but returns full request/response diagnostics.
    Use to troubleshoot empty historical data responses.
    """
    try:
        data = request.get_json() or {}
        symbols = data.get("symbols") or []
        fields = data.get("fields") or []
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        request_info = {
            "symbols": symbols,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date,
            "normalized": [_normalize_bloomberg_ticker(s) for s in symbols],
            "variants": {s: _get_canadian_ticker_variants(_normalize_bloomberg_ticker(s)) for s in symbols},
        }

        if not symbols or not fields:
            return jsonify({"error": "symbols and fields are required", "request": request_info}), 400

        results = {}
        for ticker in symbols:
            normalized = _normalize_bloomberg_ticker(ticker)
            try:
                records = bloomberg_client.get_historical_data(
                    ticker=normalized,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date
                )
                results[ticker] = {
                    "record_count": len(records),
                    "sample": records[0] if records else None,
                    "requested_as": normalized,
                }
            except Exception as e:
                results[ticker] = {"error": str(e), "requested_as": normalized}

        return jsonify({
            "request": request_info,
            "results": results,
            "note": "Set env DATA_BRIDGE_DEBUG=1 for verbose BLPAPI logging in Data Bridge console",
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/historical", methods=["POST"])
def historical():
    """Bloomberg historical data - format: { symbols, fields, start_date?, end_date? }."""
    try:
        data = request.get_json() or {}
        symbols = data.get("symbols") or []
        fields = data.get("fields") or []
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        if not symbols or not fields:
            return jsonify({"error": "symbols and fields are required"}), 400

        # US Flash PMI: no override (RELEASE_STAGE_OVERRIDE=P fails in BDH). Use BDP + BDH from Edge Function.
        historical_data = {}
        errors = []
        for ticker in symbols:
            normalized = _normalize_bloomberg_ticker(ticker)
            variants = _get_canadian_ticker_variants(normalized)
            records = []
            last_error = None
            for ticker_to_try in variants:
                try:
                    records = bloomberg_client.get_historical_data(
                        ticker=ticker_to_try,
                        fields=fields,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if records:
                        print(f"[DataBridge historical] {ticker}: {len(records)} records (ticker '{ticker_to_try}')")
                        break
                except Exception as e:
                    last_error = e
                    print(f"[DataBridge historical] {ticker}: '{ticker_to_try}' failed - {e}")
            if not records and last_error:
                errors.append(f"{ticker}: {str(last_error)}")
            if not records:
                print(
                    f"[DataBridge historical] {ticker}: 0 records from Bloomberg (tried: {variants}). "
                    f"Request: start_date={start_date!r} end_date={end_date!r} fields={fields}"
                )
            historical_data[ticker] = records  # Key by original ticker for caller
        return jsonify({"historical_data": historical_data, "errors": errors}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/reference", methods=["POST"])
def reference():
    """Bloomberg reference/EOD data - format: { symbols, fields }. Returns { reference_data: { TICKER: [{date, ...}] } }."""
    try:
        data = request.get_json() or {}
        symbols = data.get("symbols") or []
        fields = data.get("fields") or []
        if not symbols or not fields:
            return jsonify({"error": "symbols and fields are required"}), 400

        ref_data = bloomberg_client.get_reference_data(tickers=symbols, fields=fields)
        # Update expects reference_data[ticker] = [row] (array of rows)
        reference_data = {}
        errors = []
        today = datetime.now().strftime("%Y-%m-%d")
        for ticker, row in ref_data.items():
            if "error" in row:
                errors.append(f"{ticker}: {row['error']}")
            else:
                row_with_date = dict(row)
                row_with_date.setdefault("date", today)
                reference_data[ticker] = [row_with_date]
        return jsonify({"reference_data": reference_data, "errors": errors}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# SGGG / PSC portfolio endpoint (tunneled from Supabase like Bloomberg)
# Requires: OpenVPN connected so PSC is reachable; pyodbc + ODBC DSN=PSC_VIEWER
# ---------------------------------------------------------------------------
@app.route("/sggg/portfolio", methods=["GET", "POST"])
def sggg_portfolio():
    """
    Fetch Fund NAV and full position report from SGGG (PSC) for dashboard portfolio.
    Uses full EHP Select Alt query (FetchEHPSelectAltData): all strategies except
    Risk Arbitrage, Issuance Arbitrage, SOAR; SECURITY_TYPE in Stock, EquityOption, LeveragedETF, Futures.
    Called by Supabase Edge Function refresh-portfolio via the same tunnel as Bloomberg.
    """
    try:
        data = request.get_json(silent=True) or {}
        query_date = request.args.get("date") or data.get("date")
        if query_date:
            query_date = query_date.replace("-", "")[:8]
        else:
            query_date = datetime.now().strftime("%Y%m%d")

        try:
            import pyodbc
        except ImportError:
            return jsonify({
                "error": "pyodbc not installed. pip install pyodbc and configure ODBC DSN=PSC_VIEWER.",
                "fund_nav": None,
                "nav_date": None,
                "positions": []
            }), 503

        conn = None
        try:
            conn = pyodbc.connect("DSN=PSC_VIEWER")
            cursor = conn.cursor()

            # Full report query (same as FetchEHPSelectAltData)
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
            if rows and rows[0] and rows[0][19] is not None:
                try:
                    fund_nav = float(rows[0][19])
                except (TypeError, ValueError, IndexError):
                    pass
            nav_date_iso = f"{query_date[:4]}-{query_date[4:6]}-{query_date[6:8]}" if len(query_date) == 8 else query_date

            # USD/CAD rate for converting position values to CAD (fund is base CAD). Optional env override.
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
                pct_nav = (exposure / fund_nav * 100) if (fund_nav and fund_nav != 0 and exposure is not None) else None
                positions.append({
                    "strategy": _str(row[0]),
                    "trade_group": _str(row[1]),
                    "company_symbol": _str(row[2]),
                    "description": _str(row[3]),
                    "security_type": _str(row[4]),
                    "currency": _str(row[5]),
                    "bbg_ticker": _str(row[6]),
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
                    "profit": None,  # PSC query doesn't have Profit column; can add if available
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
            "positions": []
        }), 500


# ---------------------------------------------------------------------------
# SGGG Diamond API endpoints (runs in parallel with PSC/ODBC)
# Requires: SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD, SGGG_DIAMOND_FUND_ID in env
# ---------------------------------------------------------------------------
@app.route("/sggg/diamond/portfolio", methods=["GET", "POST"])
def sggg_diamond_portfolio():
    """
    Get finalized portfolio from SGGG Diamond API.
    Body or query: fund_id (optional, uses SGGG_DIAMOND_FUND_ID if not provided), valuation_date (yyyy-mm-dd)
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD, SGGG_DIAMOND_FUND_ID."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = request.args.get("fund_id") or data.get("fund_id") or os.environ.get("SGGG_DIAMOND_FUND_ID")
        valuation_date = request.args.get("date") or request.args.get("valuation_date") or data.get("date") or data.get("valuation_date")
        if not valuation_date:
            valuation_date = datetime.now().strftime("%Y-%m-%d")
        else:
            valuation_date = valuation_date.replace("-", "")[:8]
            valuation_date = f"{valuation_date[:4]}-{valuation_date[4:6]}-{valuation_date[6:8]}"
        if not fund_id:
            return jsonify({"error": "fund_id required (or set SGGG_DIAMOND_FUND_ID)"}), 400
        result = client.get_portfolio(fund_id=fund_id, valuation_date=valuation_date)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/sggg/diamond/trades", methods=["GET", "POST"])
def sggg_diamond_trades():
    """
    Get portfolio trades from SGGG Diamond API.
    Body or query: fund_id (required or SGGG_DIAMOND_FUND_ID), start_date, end_date (yyyy-mm-dd, max 1 month range)
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = request.args.get("fund_id") or data.get("fund_id") or os.environ.get("SGGG_DIAMOND_FUND_ID")
        start_date = request.args.get("start_date") or data.get("start_date")
        end_date = request.args.get("end_date") or data.get("end_date")
        if not fund_id:
            return jsonify({"error": "fund_id required (or set SGGG_DIAMOND_FUND_ID)"}), 400
        result = client.get_portfolio_trades(
            fund_parent_id=fund_id,
            start_date=start_date,
            end_date=end_date,
        )
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/economic-calendar", methods=["POST"])
def economic_calendar():
    """
    Fetch economic calendar data for given tickers or all configured tickers.
    Uses BDP (Bloomberg Data Point) to get current/future release data.
    """
    try:
        data = request.get_json() or {}
        # Get tickers from request body if provided, otherwise load from database
        tickers = data.get("tickers", [])
        
        if not tickers:
            # Fallback: load from database
            try:
                result = supabase.table("economic_calendar_tickers").select("ticker").execute()
                if result.data:
                    tickers = [row["ticker"] for row in result.data]
                    print(f"Loaded {len(tickers)} tickers from database")
            except Exception as e:
                print(f"Could not load tickers from database: {e}")
                # Fallback: load from file if exists
                ticker_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "economic_calendar_tickers.txt")
                if os.path.exists(ticker_file):
                    with open(ticker_file, 'r', encoding='utf-8') as f:
                        tickers = [line.strip() for line in f if line.strip()]
                    print(f"Loaded {len(tickers)} tickers from file")
        
        if not tickers:
            return jsonify({
                "success": False,
                "error": "No tickers provided and no tickers configured. Please provide tickers in request body or import economic_calendar_tickers.sql",
                "calendar_data": []
            }), 400
        
        # Debug: log exact request for troubleshooting
        print(f"[economic-calendar] REQUEST: tickers_count={len(tickers)} tickers={tickers[:30]}{'...' if len(tickers) > 30 else ''}")
        
        # Date range: today to today + 365 days
        today = datetime.now().date()
        end_date = today + timedelta(days=365)
        today_str = today.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        print(f"Fetching economic calendar data from {today_str} to {end_date_str}")
        
        # Bloomberg fields needed (based on Excel formulas)
        fields = [
            "REGION_OR_COUNTRY",      # Country
            "SECURITY_DES",           # Event description
            "ECO_RELEASE_DT",         # Release date
            "ECO_RELEASE_TIME",       # Release time
            "OBSERVATION_PERIOD",     # Period
            "RT_BN_SURVEY_MEDIAN",    # Survey median
            "PX_LAST",                # Actual value (current)
            "PREV_CLOSE_VAL",         # Prior value
            "FIRST_REVISION",         # Revised value
            "LAST_UPDATE_DT",         # Last update date
            "PREV_TRADING_DT_REALTIME",  # Last report date
            "PRIOR_OBSERVATION_DATE",    # Prior observation date
        ]
        
        # For future dates, we need to use ECO_FUTURE_RELEASE_DATE
        # For current/past dates, we use the regular fields
        
        events = []
        errors = []
        
        # Process tickers in batches to avoid overwhelming Bloomberg
        BATCH_SIZE = 50
        for i in range(0, len(tickers), BATCH_SIZE):
            batch_tickers = tickers[i:i+BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(f"Processing batch {batch_num} ({len(batch_tickers)} tickers)...")
            print(f"[economic-calendar] BATCH {batch_num} REQUEST: tickers={batch_tickers[:5]}{'...' if len(batch_tickers) > 5 else ''} fields={fields}")
            
            try:
                # Fetch reference data for this batch
                reference_data = bloomberg_client.get_reference_data(
                    tickers=batch_tickers,
                    fields=fields
                )
                # Debug: log what Bloomberg returned for this batch
                for t, row in reference_data.items():
                    status = "error" if "error" in row else "ok"
                    eco_dt = row.get("ECO_RELEASE_DT") if isinstance(row, dict) else None
                    print(f"[economic-calendar] BATCH {batch_num} RESPONSE: ticker={t} status={status} ECO_RELEASE_DT={eco_dt!r} keys={list(row.keys())[:8] if isinstance(row, dict) else 'n/a'}")
                if batch_tickers and batch_tickers[0] in reference_data:
                    sample = reference_data[batch_tickers[0]]
                    print(f"[economic-calendar] BATCH {batch_num} SAMPLE (first ticker): {sample}")
                
                # Process each ticker's data
                for ticker, data in reference_data.items():
                    if "error" in data:
                        errors.append(f"{ticker}: {data['error']}")
                        continue
                    
                    # Extract release date - try ECO_FUTURE_RELEASE_DATE first for future events
                    release_date = None
                    release_time = None
                    
                    # Priority: Use ECO_RELEASE_DT if it's today or in the past (current release)
                    # Only use ECO_FUTURE_RELEASE_DATE if ECO_RELEASE_DT is not today/past
                    is_future_release = False  # Track if we got the date from ECO_FUTURE_RELEASE_DATE
                    
                    # First, try ECO_RELEASE_DT (current/past release)
                    if "ECO_RELEASE_DT" in data:
                        release_dt = data["ECO_RELEASE_DT"]
                        if release_dt:
                            if isinstance(release_dt, datetime):
                                release_date = release_dt.date()
                            elif isinstance(release_dt, str):
                                try:
                                    release_date = datetime.strptime(release_dt[:8], "%Y%m%d").date()
                                except:
                                    pass
                            
                            # If ECO_RELEASE_DT is today or in the past, use it (not a future event)
                            if release_date and release_date <= today:
                                is_future_release = False
                    
                    # If we don't have a release date yet, or if ECO_RELEASE_DT was in the future,
                    # try ECO_FUTURE_RELEASE_DATE (next future release)
                    if (not release_date or (release_date and release_date > today)):
                        try:
                            future_release = bloomberg_client.get_reference_data(
                                tickers=[ticker],
                                fields=["ECO_FUTURE_RELEASE_DATE"]
                            )
                            if ticker in future_release and "ECO_FUTURE_RELEASE_DATE" in future_release[ticker]:
                                future_date = future_release[ticker]["ECO_FUTURE_RELEASE_DATE"]
                                if future_date:
                                    # Convert Bloomberg date to Python date
                                    if isinstance(future_date, datetime):
                                        future_release_date = future_date.date()
                                    elif isinstance(future_date, str):
                                        # Try parsing various formats
                                        try:
                                            future_release_date = datetime.strptime(future_date[:8], "%Y%m%d").date()
                                        except:
                                            future_release_date = None
                                    
                                    # Only use future date if we don't have a current release date
                                    if future_release_date and not release_date:
                                        release_date = future_release_date
                                        is_future_release = True
                                    # If we have a current release date that's today, keep it and ignore future date
                        except Exception as e:
                            pass
                    
                    # Filter by date range
                    # Include all events from today onwards (even if time has passed today)
                    # Events only roll off after the date has passed (i.e., tomorrow)
                    if release_date:
                        if release_date < today or release_date > end_date:
                            continue  # Skip events outside our date range
                        # Note: We keep all events for today, regardless of whether the time has passed
                    
                    # Extract release time
                    if "ECO_RELEASE_TIME" in data and data["ECO_RELEASE_TIME"]:
                        release_time = str(data["ECO_RELEASE_TIME"])
                    
                    # Determine if this is a future event (check both date AND time)
                    # If we got the date from ECO_FUTURE_RELEASE_DATE, it's definitely a future event
                    # Otherwise, check if release_date > today OR (release_date == today AND release_time hasn't passed)
                    is_future_event = False
                    if is_future_release:
                        is_future_event = True
                    elif release_date:
                        if release_date > today:
                            is_future_event = True
                        elif release_date == today and release_time:
                            # For today's events, check if the release time has passed
                            # Bloomberg ECO_RELEASE_TIME is in Eastern Time (EST/EDT)
                            # Parse release_time (format: "HH:MM:SS" or "08:30:00")
                            try:
                                time_parts = release_time.split(":")
                                if len(time_parts) >= 2:
                                    release_hour = int(time_parts[0])
                                    release_minute = int(time_parts[1])
                                    
                                    # Create release datetime in Eastern Time
                                    # Bloomberg times are in Eastern Time (America/New_York)
                                    release_time_obj = datetime.min.time().replace(
                                        hour=release_hour, minute=release_minute, second=0
                                    )
                                    release_dt_naive = datetime.combine(release_date, release_time_obj)
                                    
                                    if HAS_ZONEINFO:
                                        # Use zoneinfo (Python 3.9+)
                                        eastern_tz = ZoneInfo("America/New_York")
                                        # Create timezone-aware datetime
                                        release_dt_eastern = release_dt_naive.replace(tzinfo=eastern_tz)
                                        # Get current time in Eastern Time
                                        now_eastern = datetime.now(eastern_tz)
                                        
                                        # Compare: if current time < release time, it's a future event
                                        if now_eastern < release_dt_eastern:
                                            is_future_event = True
                                    elif HAS_PYTZ:
                                        # Use pytz for timezone handling
                                        eastern_tz = pytz.timezone("America/New_York")
                                        # Create release datetime in Eastern Time using localize
                                        release_dt_eastern = eastern_tz.localize(release_dt_naive)
                                        # Get current time in Eastern Time
                                        now_eastern = datetime.now(eastern_tz)
                                        
                                        # Compare: if current time < release time, it's a future event
                                        if now_eastern < release_dt_eastern:
                                            is_future_event = True
                                    else:
                                        # Fallback: use local time (assumes system is in Eastern Time)
                                        now = datetime.now()
                                        current_hour = now.hour
                                        current_minute = now.minute
                                        if current_hour < release_hour or (current_hour == release_hour and current_minute < release_minute):
                                            is_future_event = True
                            except (ValueError, IndexError, Exception) as e:
                                # If we can't parse the time, assume it's not a future event
                                print(f"Warning: Could not parse release_time '{release_time}': {e}")
                                pass
                    
                    # For future events, actual should be NULL (no actual value yet)
                    # PX_LAST for future events might contain the prior period's value or survey value, not the actual for the future date
                    # For past/current events (where time has passed), use PX_LAST (current actual value)
                    # Note: PX_LAST from ReferenceDataRequest gives the current/latest value,
                    # not the historical value as of the release date
                    actual_value = None
                    if not is_future_event:
                        actual_value = _safe_float(data.get("PX_LAST"))
                    
                    # Build event record
                    event = {
                        "ticker": ticker,
                        "country": data.get("REGION_OR_COUNTRY", ""),
                        "event": data.get("SECURITY_DES", ""),
                        "release_date": release_date.strftime("%Y-%m-%d") if release_date else None,
                        "release_time": release_time,
                        "period": data.get("OBSERVATION_PERIOD", ""),
                        "survey_median": _safe_float(data.get("RT_BN_SURVEY_MEDIAN")),
                        "actual": actual_value,  # NULL for future events, PX_LAST for past/current
                        "prior": _safe_float(data.get("PREV_CLOSE_VAL")),
                        "revised": _safe_float(data.get("FIRST_REVISION")),
                        "last_update_date": _parse_date(data.get("LAST_UPDATE_DT")),
                        "last_report_date": _parse_date(data.get("PREV_TRADING_DT_REALTIME")),
                        "prior_observation_date": _parse_date(data.get("PRIOR_OBSERVATION_DATE")),
                    }
                    
                    # Only add if we have a release date
                    if event["release_date"]:
                        events.append(event)
                
            except Exception as e:
                error_msg = f"Error processing batch {i//BATCH_SIZE + 1}: {str(e)}"
                print(f"  [FAIL] {error_msg}")
                errors.append(error_msg)
                traceback.print_exc()
        
        print(f"Fetched {len(events)} economic calendar events")
        
        # Return calendar_data to match Edge Function expectations
        # Also include events for backward compatibility
        return jsonify({
            "success": True,
            "calendar_data": events,  # Primary field expected by Edge Function
            "events": events,  # Backward compatibility
            "count": len(events),
            "errors": errors,
            "date_range": {
                "from": today_str,
                "to": end_date_str
            }
        })
        
    except Exception as e:
        error_msg = f"Economic calendar error: {str(e)}"
        print(f"[FAIL] {error_msg}")
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": error_msg
        }), 500


def _safe_float(value):
    """Safely convert value to float, return None if not possible"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_date(value):
    """Parse Bloomberg date value to YYYY-MM-DD string"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.date().strftime("%Y-%m-%d")
        elif isinstance(value, str):
            # Try parsing various formats
            if len(value) >= 8:
                return datetime.strptime(value[:8], "%Y%m%d").date().strftime("%Y-%m-%d")
    except:
        pass
    return None


# ---------------------------------------------------------------------------
# /quotes - wealth-scope compatibility (same format as old bloomberg bridge)
# Returns last_price, open_price, high_price, low_price, volume, etc. for edge function
# ---------------------------------------------------------------------------
@app.route("/quotes", methods=["POST"])
def quotes():
    """Bloomberg real-time quotes - wealth-scope format: { symbols: [], security_types?: [] }."""
    try:
        data = request.get_json() or {}
        symbols = data.get("symbols") or data.get("tickers") or []
        if not symbols:
            return jsonify({"quotes": {}, "errors": ["symbols or tickers required"]}), 400
        symbols = [str(s).strip() for s in symbols if s]
        if not symbols:
            return jsonify({"quotes": {}, "errors": []}), 200
        fields = [
            "PX_LAST", "PX_BID", "PX_ASK", "PREV_CLOSE_VAL", "VOLUME",
            "PX_OPEN", "PX_HIGH", "PX_LOW", "CHG_PCT_1D", "CHG_NET_1D",
            "CRNCY", "NAME", "ID_EXCH_SYMBOL"
        ]
        reference_data = bloomberg_client.get_reference_data(tickers=symbols, fields=fields)
        quotes_result = {}
        for ticker, row in reference_data.items():
            if "error" in row:
                continue
            # Map PX_* to wealth-scope/edge-function expected format (matches old bloomberg bridge)
            current_price = row.get("PX_LAST") or row.get("PX_BID")
            quote = {
                "symbol": ticker,
                "last_price": current_price,
                "bid": row.get("PX_BID"),
                "ask": row.get("PX_ASK"),
                "close_price": row.get("PREV_CLOSE_VAL"),
                "volume": row.get("VOLUME"),
                "open_price": row.get("PX_OPEN"),
                "high_price": row.get("PX_HIGH"),
                "low_price": row.get("PX_LOW"),
                "change_percent": row.get("CHG_PCT_1D"),
                "change_amount": row.get("CHG_NET_1D"),
                "currency": row.get("CRNCY"),
                "name": row.get("NAME"),
                "exchange": row.get("ID_EXCH_SYMBOL"),
            }
            quotes_result[ticker] = {k: v for k, v in quote.items() if v is not None}
        return jsonify({"quotes": quotes_result, "errors": []}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"quotes": {}, "errors": [str(e)]}), 500


# ---------------------------------------------------------------------------
# /clarifi/process and /ehp/process - market-dashboard update Edge Function
# ---------------------------------------------------------------------------
@app.route("/clarifi/process", methods=["POST"])
def clarifi_process():
    """Process Clarifi files from CLARIFI_DIR and upload to Supabase."""
    try:
        from clarifi_processor import process_clarifi_file
        clarifi_path = Path(CLARIFI_DIR)
        if not clarifi_path.exists():
            return jsonify({"error": f"Clarifi directory not found: {CLARIFI_DIR}", "inserted": 0, "errors": []}), 404
        files_to_process = []
        for p in clarifi_path.glob("*.txt"):
            if 'macrodataexport' in p.name.lower() or 'diffusionindexexport' in p.name.lower():
                files_to_process.append(p)
        for p in clarifi_path.glob("*.csv"):
            if 'oildemand' in p.name.lower():
                files_to_process.append(p)
        if not files_to_process:
            return jsonify({"inserted": 0, "errors": [], "files_processed": [], "message": "No Clarifi files found"}), 200
        total_inserted = 0
        all_errors = []
        files_processed = []
        for file_path in files_to_process:
            result = process_clarifi_file(file_path, supabase)
            total_inserted += result["inserted"]
            all_errors.extend(result["errors"])
            files_processed.append({"file": file_path.name, "inserted": result["inserted"], "errors": result["errors"]})
        return jsonify({"inserted": total_inserted, "errors": all_errors, "files_processed": files_processed}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"inserted": 0, "errors": [str(e)]}), 500


@app.route("/clarifi/list", methods=["GET"])
def clarifi_list():
    """List available Clarifi files."""
    clarifi_path = Path(CLARIFI_DIR)
    if not clarifi_path.exists():
        return jsonify({"error": f"Clarifi directory not found: {CLARIFI_DIR}"}), 404
    files = []
    for p in clarifi_path.glob("*.txt"):
        if 'macrodataexport' in p.name.lower() or 'diffusionindexexport' in p.name.lower():
            files.append({"name": p.name, "size": p.stat().st_size, "modified": datetime.fromtimestamp(p.stat().st_mtime).isoformat()})
    return jsonify({"files": files}), 200


@app.route("/ehp/process", methods=["POST"])
def ehp_process():
    """Read EHP files and return contents for Edge Function processing."""
    clarifi_path = Path(CLARIFI_DIR)
    if not clarifi_path.exists():
        return jsonify({"error": f"Directory not found: {CLARIFI_DIR}"}), 404
    files_to_read = [
        ("ehpRanksLongsData", "HF_Flow_100_longs_Ranks.TXT"), ("ehpRanksShortsData", "HF_Flow_100_shorts_Ranks.TXT"),
        ("ehpReturnsLongsData", "HF_Flow_100_longs_Returns.TXT"), ("ehpReturnsShortsData", "HF_Flow_100_shorts_Returns.TXT"),
        ("ehpReturns15050Data", "HF_Flow_150_50_Returns.txt"),
        ("ehpRanksLongsTsxData", "hf_flow_tsx_100_longs_Ranks.TXT"), ("ehpRanksShortsTsxData", "hf_flow_tsx_100_shorts_Ranks.TXT"),
        ("ehpReturnsLongsTsxData", "hf_flow_tsx_100_longs_Returns.TXT"), ("ehpReturnsShortsTsxData", "hf_flow_tsx_100_shorts_Returns.TXT"),
        ("ehpReturns15050TsxData", "hf_flow_tsx_150_50_Returns.TXT"),
        ("divForecastRanksLongsData", "div_forecast_100_longs_Ranks.TXT"), ("divForecastRanksShortsData", "div_forecast_100_shorts_Ranks.TXT"),
        ("divForecastReturnsLongsData", "div_forecast_100_longs_Returns.TXT"), ("divForecastReturnsShortsData", "div_forecast_100_shorts_Returns.TXT"),
        ("divForecastReturns15050Data", "div_forecast_150_50_Returns.txt"),
        ("divForecastRanksLongsTsxData", "div_forecast_tsx_100_longs_Ranks.TXT"), ("divForecastRanksShortsTsxData", "div_forecast_tsx_100_shorts_Ranks.TXT"),
        ("divForecastReturnsLongsTsxData", "div_forecast_tsx_100_longs_Returns.TXT"), ("divForecastReturnsShortsTsxData", "div_forecast_tsx_100_shorts_Returns.TXT"),
        ("divForecastReturns15050TsxData", "div_forecast_tsx_150_50_Returns.TXT"),
    ]
    result = {data_key: None for data_key, _ in files_to_read}
    result["files_processed"] = []
    result["errors"] = []
    for data_key, file_name in files_to_read:
        file_path = clarifi_path / file_name
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    result[data_key] = f.read()
                result["files_processed"].append({"file": file_name, "found": True, "size": len(result[data_key])})
            except Exception as e:
                result["errors"].append(str(e))
                result["files_processed"].append({"file": file_name, "found": True, "error": str(e)})
        else:
            result["files_processed"].append({"file": file_name, "found": False})
    return jsonify(result), 200


if __name__ == "__main__":
    try:
        print("=" * 60)
        print("Data Bridge Service")
        print("=" * 60)
        print(f"Bloomberg Client: {type(bloomberg_client).__name__}")
        print("Note: Using BLPAPI (BQL only available in BQuant IDE)")
        print(f"Supabase URL: {SUPABASE_URL}")
        print(f"Listening on http://127.0.0.1:{SERVICE_PORT}")
        print()
        print("Endpoints: /health, /bloomberg-update, /bloomberg/quotes, /quotes, /historical, /historical-debug, /reference,")
        print("  /economic-calendar, /clarifi/process, /clarifi/list, /ehp/process, /sggg/portfolio")
        print("SGGG requires: OpenVPN + DSN=PSC_VIEWER + pyodbc")
        print()
        print("Service is running. Press Ctrl+C to stop.")
        print("=" * 60)
    except:
        pass
    
    try:
        app.run(host="127.0.0.1", port=SERVICE_PORT, debug=False, use_reloader=False)
    except Exception as e:
        print(f"Error starting service: {e}")
        traceback.print_exc()
        sys.exit(1)

