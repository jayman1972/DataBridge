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
import math
import logging
import traceback
import time
import threading
import re
from datetime import datetime, timedelta, timezone, time as dt_time, date as date_type
from pathlib import Path
from typing import Dict, List, Optional, Any
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
import requests
import secrets

# Suppress SSL warning for IBKR Gateway self-signed cert (localhost:5001)
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

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
                        elif key == "SGGG_DIAMOND_FUND_IDS":
                            os.environ["SGGG_DIAMOND_FUND_IDS"] = value
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
                        if k in ("SGGG_DIAMOND_USERNAME", "SGGG_DIAMOND_PASSWORD", "SGGG_DIAMOND_FUND_ID", "SGGG_DIAMOND_FUND_IDS") and v:
                            os.environ[k] = v
        except Exception as e:
            print(f"Error reading SGGG config from {_cfg}: {e}")
        break

# Uses BLPAPI (BQL only available in BQuant IDE)
SERVICE_PORT = int(os.getenv("PORT", "5000"))
# Bump when debugging deploy mismatches (curl /health to confirm running build)
DATA_BRIDGE_BUILD = "2026-04-03-polymarket-alert-webhook"

_ecal_logger = logging.getLogger("data_bridge.economic_calendar")
if not _ecal_logger.handlers:
    _ecal_handler = logging.StreamHandler(sys.stderr)
    _ecal_handler.setFormatter(
        logging.Formatter("%(asctime)s [ecal] %(levelname)s %(message)s")
    )
    _ecal_logger.addHandler(_ecal_handler)
    _ecal_logger.setLevel(logging.INFO)
    _ecal_logger.propagate = False

_bbg_logger = logging.getLogger("data_bridge.bloomberg")
if not _bbg_logger.handlers:
    _bbg_handler = logging.StreamHandler(sys.stderr)
    _bbg_handler.setFormatter(
        logging.Formatter("%(asctime)s [bbg] %(levelname)s %(message)s")
    )
    _bbg_logger.addHandler(_bbg_handler)
    _bbg_logger.setLevel(logging.INFO)
    _bbg_logger.propagate = False


def _bbg_verbose_full() -> bool:
    """Log full serialized JSON for /reference and extra BLPAPI lines (large)."""
    e = os.environ.get("DATA_BRIDGE_BLOOMBERG_VERBOSE", "").lower()
    if e in ("1", "true", "yes"):
        return True
    return os.environ.get("DATA_BRIDGE_DEBUG", "").lower() in ("1", "true", "yes")


# IBKR Client Portal Gateway (tickle keeps session alive; must use port 5001 vs Data Bridge 5000)
IBKR_GATEWAY_URL = os.getenv("IBKR_GATEWAY_URL", "https://localhost:5001").rstrip("/")
_ibkr_session = requests.Session()
_ibkr_session.verify = False
_ibkr_session.headers.update({"User-Agent": "Console"})  # Gateway may require this to accept server-side requests
# Optional: send browser session cookie so Gateway accepts requests (log in at https://localhost:5001, copy Cookie from DevTools)
_ibkr_cookie_str = os.getenv("IBKR_SESSION_COOKIE", "").strip()
if _ibkr_cookie_str:
    from urllib.parse import urlparse
    _ibkr_netloc = urlparse(IBKR_GATEWAY_URL).netloc.split(":")[0]  # e.g. localhost
    for part in _ibkr_cookie_str.split(";"):
        part = part.strip()
        if "=" in part:
            _name, _val = part.split("=", 1)
            _ibkr_session.cookies.set(_name.strip(), _val.strip().strip('"'), domain=_ibkr_netloc, path="/")
_ibkr_rate_lock = threading.Lock()
_ibkr_last_request_time = 0.0
_ibkr_history_semaphore = threading.Semaphore(5)  # max 5 concurrent history requests

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

SGGG_FUND_ID_TO_PSC_PORTFOLIO: Dict[str, str] = {
    # Matches the fund list hardcoded in market-dashboard Fund Admin NAV reports.
    # You can override by sending "portfolio" in the request body.
    "415a3530-3034-4536-4432-303030364337": "EHP Adv Alt",
    "41010000-7F7A-0A65-D559-45484608DB40": "EHP Tactical Growth Alt",
    "41323030-3031-4144-3637-303030364338": "EHP Select Alt",
    "41010000-7F2A-D7E8-776F-45484608D91C": "EHP Strategic Income Alt",
    "01010000-801A-4995-8370-45484608DE57": "Exponential Balanced Growth Fund",
}


def _ymd_to_compact(s: str) -> str:
    """YYYY-MM-DD -> YYYYMMDD (or accept YYYYMMDD)."""
    if not s:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return (s[:10]).replace("-", "")
    return s.replace("-", "")[:8]


def _compact_to_ymd(s: str) -> Optional[str]:
    s = _ymd_to_compact(s)
    if len(s) != 8:
        return None
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _psc_portfolio_from_request(data: dict) -> Optional[str]:
    portfolio = (data.get("portfolio") or "").strip()
    if portfolio:
        return portfolio
    fund_id = (data.get("fund_id") or "").strip()
    if fund_id and fund_id in SGGG_FUND_ID_TO_PSC_PORTFOLIO:
        return SGGG_FUND_ID_TO_PSC_PORTFOLIO[fund_id]
    return None


def _like_patterns_from_fund_name(fund_name: Optional[str]) -> List[str]:
    """
    Build a few MySQL LIKE patterns from a fund display name.
    Example: "EHP Tactical Growth Alternative Fund" -> ["%Tactical%Growth%", "%Tactical%", "%Growth%"].
    """
    if not fund_name:
        return []
    name = str(fund_name).strip()
    if not name:
        return []
    # Remove common noise words
    stop = {"EHP", "FUND", "ALTERNATIVE", "ALTERNATIVES", "THE", "SERIES", "TRUST"}
    words = [w for w in re.split(r"[^A-Za-z0-9]+", name) if w]
    keep = [w for w in words if w.upper() not in stop and len(w) >= 4]
    if not keep:
        return []
    patterns: List[str] = []
    if len(keep) >= 2:
        patterns.append("%" + "%".join(keep[:3]) + "%")
    patterns.extend([f"%{w}%" for w in keep[:3]])
    # Dedup preserving order
    out: List[str] = []
    seen = set()
    for p in patterns:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def _pyodbc_or_503():
    try:
        import pyodbc  # type: ignore
        return pyodbc, None
    except ImportError:
        return None, (
            jsonify({
                "error": "pyodbc not installed. pip install pyodbc and configure ODBC DSN=PSC_VIEWER.",
            }),
            503,
        )



def _json_response(payload: dict, status: int = 200) -> Response:
    """JSON response that never raises on odd types (Flask jsonify can still fail on some values)."""
    return Response(
        json.dumps(payload, default=str, ensure_ascii=False),
        status=status,
        mimetype="application/json; charset=utf-8",
    )


def _json_scalar_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


@app.after_request
def add_cors_headers(response):
    response.headers.add("Access-Control-Allow-Private-Network", "true")
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


@app.errorhandler(Exception)
def _json_errors_for_economic_calendar(exc: BaseException):
    """Uncaught exceptions on POST /economic-calendar otherwise become HTML 502 via ngrok."""
    if isinstance(exc, HTTPException):
        return exc
    path = (request.path or "").rstrip("/")
    if path != "/economic-calendar" or request.method != "POST":
        raise exc
    _ecal_logger.error(
        "uncaught_exception %s: %s",
        type(exc).__name__,
        exc,
        exc_info=True,
    )
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    payload: Dict[str, Any] = {
        "success": False,
        "service": "DataBridge",
        "build": DATA_BRIDGE_BUILD,
        "calendar_data": [],
        "error": str(exc),
        "exception_type": type(exc).__name__,
    }
    if os.environ.get("DATA_BRIDGE_EXPOSE_TRACEBACK", "").lower() in ("1", "true", "yes"):
        payload["traceback"] = traceback.format_exc()
    try:
        return _json_response(payload, 500)
    except Exception as enc_err:
        _ecal_logger.error("failed to json-serialize error payload: %s", enc_err, exc_info=True)
        minimal = json.dumps(
            {
                "success": False,
                "error": str(exc),
                "exception_type": type(exc).__name__,
                "build": DATA_BRIDGE_BUILD,
                "calendar_data": [],
            },
            default=str,
        )
        return Response(
            minimal,
            status=500,
            mimetype="application/json; charset=utf-8",
        )


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
        "build": DATA_BRIDGE_BUILD,
        "port": SERVICE_PORT,
        "bloomberg": client_info,
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

        # Full request log (stdout + stderr logger for market-tab refresh debugging)
        print(f"[DataBridge historical] REQUEST (full): symbols={symbols} fields={fields} start_date={start_date!r} end_date={end_date!r}", flush=True)
        _bbg_logger.info(
            "historical REQUEST symbols=%d fields=%s start_date=%s end_date=%s sample_symbols=%s",
            len(symbols),
            fields,
            start_date,
            end_date,
            symbols[:8] if len(symbols) > 8 else symbols,
        )

        # US Flash PMI: no override (RELEASE_STAGE_OVERRIDE=P fails in BDH). Use BDP + BDH from Edge Function.
        historical_data = {}
        errors = []
        for ticker in symbols:
            normalized = _normalize_bloomberg_ticker(ticker)
            variants = _get_canadian_ticker_variants(normalized)
            records = []
            last_error = None
            print(
                f"[DataBridge historical] REQUEST ticker={ticker!r} variants={variants} fields={fields} start_date={start_date!r} end_date={end_date!r}",
                flush=True,
            )
            _bbg_logger.info(
                "historical ticker=%r variants=%s fields=%s start=%s end=%s",
                ticker,
                variants,
                fields,
                start_date,
                end_date,
            )
            for ticker_to_try in variants:
                try:
                    records = bloomberg_client.get_historical_data(
                        ticker=ticker_to_try,
                        fields=fields,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if records:
                        ser = _log_serialize(records)
                        print(
                            f"[DataBridge historical] RESPONSE ticker={ticker!r} record_count={len(records)} records={ser}",
                            flush=True,
                        )
                        _bbg_logger.info(
                            "historical RESPONSE ticker=%r record_count=%d (full JSON: DATA_BRIDGE_BLOOMBERG_VERBOSE=1)",
                            ticker,
                            len(records),
                        )
                        if _bbg_verbose_full():
                            _bbg_logger.info(
                                "historical RESPONSE body ticker=%r json=%s",
                                ticker,
                                json.dumps(ser, default=str),
                            )
                        break
                except Exception as e:
                    last_error = e
                    print(f"[DataBridge historical] RESPONSE ticker={ticker_to_try!r} exception={e!r}", flush=True)
                    _bbg_logger.error("historical exception ticker_try=%r err=%s", ticker_to_try, e, exc_info=True)
            if not records and last_error:
                errors.append(f"{ticker}: {str(last_error)}")
            if not records:
                print(
                    f"[DataBridge historical] RESPONSE ticker={ticker!r} record_count=0 last_error={last_error!r} (tried variants: {variants})",
                    flush=True,
                )
                _bbg_logger.warning(
                    "historical empty ticker=%r last_error=%r variants=%s",
                    ticker,
                    last_error,
                    variants,
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

        print(f"[DataBridge reference] REQUEST (full): symbols={symbols} fields={fields}", flush=True)
        _bbg_logger.info(
            "reference REQUEST symbols=%d fields=%s sample=%s",
            len(symbols),
            fields,
            symbols[:12] if len(symbols) > 12 else symbols,
        )
        ref_data = bloomberg_client.get_reference_data(tickers=symbols, fields=fields)
        ser = _log_serialize(ref_data)
        print(
            f"[DataBridge reference] RESPONSE (full from Bloomberg): {json.dumps(ser, default=str, indent=2)}",
            flush=True,
        )
        ok = sum(1 for r in ref_data.values() if isinstance(r, dict) and "error" not in r)
        bad = [k for k, r in ref_data.items() if isinstance(r, dict) and "error" in r]
        _bbg_logger.info(
            "reference RESPONSE tickers=%d ok=%d security_errors=%d error_keys=%s",
            len(ref_data),
            ok,
            len(bad),
            bad[:20] if len(bad) > 20 else bad,
        )
        if _bbg_verbose_full():
            _bbg_logger.info(
                "reference RESPONSE body json=%s",
                json.dumps(ser, default=str),
            )
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
        _bbg_logger.error("reference failed: %s", e, exc_info=True)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# SGGG / PSC connection test (for IP whitelist / OpenVPN checks)
# ---------------------------------------------------------------------------
@app.route("/sggg/health", methods=["GET"])
def sggg_health():
    """
    Test connectivity to SGGG PSC (ODBC DSN=PSC_VIEWER).
    Returns 200 { "status": "ok", "message": "..." } or 503 with error details.
    """
    try:
        import pyodbc
    except ImportError:
        return jsonify({
            "status": "error",
            "message": "pyodbc not installed. pip install pyodbc and configure ODBC DSN=PSC_VIEWER.",
        }), 503
    conn = None
    try:
        conn = pyodbc.connect("DSN=PSC_VIEWER")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return jsonify({
            "status": "ok",
            "message": "SGGG PSC connection successful (DSN=PSC_VIEWER).",
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
        }), 503
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


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
# SGGG / PSC options trades + exposure history (for Fund Options Tax Recon)
# ---------------------------------------------------------------------------
@app.route("/sggg/options-tax-reconciliation", methods=["POST"])
def sggg_options_tax_reconciliation():
    """
    Fetch raw option trades and position history needed to compute options tax characterization.

    Body:
      - fund_id: Diamond fund id GUID (preferred)
      - portfolio: PSC portfolio name override (optional)
      - start_date: YYYY-MM-DD
      - end_date: YYYY-MM-DD

    Returns:
      { portfolio, start_date, end_date, option_trades: [...], position_history: [...] }
    """
    data = request.get_json(silent=True) or {}
    fund_name = (data.get("fund_name") or "").strip() or None
    portfolio = _psc_portfolio_from_request(data)
    if not portfolio and not fund_name:
        return jsonify({"error": "portfolio required (or pass fund_id mapped to a PSC portfolio, or include fund_name for auto-detect)"}), 400

    start_date = _compact_to_ymd(data.get("start_date") or "")
    end_date = _compact_to_ymd(data.get("end_date") or "")
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_compact = _ymd_to_compact(start_date)
    end_compact = _ymd_to_compact(end_date)

    pyodbc, err = _pyodbc_or_503()
    if err:
        return err

    conn = None
    try:
        conn = pyodbc.connect("DSN=PSC_VIEWER")
        cursor = conn.cursor()

        # 1) Option trades (fills)
        orders_sql_base = (
            "SELECT "
            "ORDER_ID, PORTFOLIO, SECURITY, DESCRIPTION, STRATEGY, SEC_CCY, BBG_TICKER, "
            "TRADE_DATE_INT, TRADE_DATE_TIME, SETTLE_DATE_INT, SETTLE_DATE, `ORDER` AS ORDER_ACTION, PRICE, COMMISH, BROKER, FILLED_QTTY, "
            "ACT_QTTY, SEC_CCY_AMT, SETTLE_CCY_AMT, COMPANY_SYMBOL, CONTRACT_SIZE, "
            "COUNTRY, QUOTE_SIZE, SECTOR, SECURITY_TYPE, UNDERLYING_SECURITY, "
            "UNDERLYING_DESCRIPTION, UNDERLYING_SEC_CCY, UNDERLYING_CUSIP, UNDERLYING_ISIN, "
            "UNDERLYING_SEDOL, UNDERLYING_EXCHANGE, UNDERLYING_QUOTE_SIZE, UNDERLYING_CONTRACT_SIZE, "
            "UNDERLYING_SECTOR, UNDERLYING_COUNTRY "
            "FROM psc_filled_orders "
            "WHERE TRADE_DATE_INT BETWEEN ? AND ? "
            "AND PORTFOLIO = ? "
            "AND SECURITY_TYPE = 'EquityOption' "
            "ORDER BY ORDER_ID, TRADE_DATE_INT, SECURITY, PORTFOLIO, STRATEGY"
        )
        # If PSC has an option delta column, include it. Some environments don't, so we fall back gracefully.
        orders_sql_with_delta = orders_sql_base.replace(
            "COMPANY_SYMBOL, CONTRACT_SIZE, ",
            "COMPANY_SYMBOL, CONTRACT_SIZE, DELTA, ",
        )
        resolved_portfolio = portfolio
        portfolio_candidates: List[str] = []

        _orders_sql_to_use = orders_sql_with_delta

        def _run_orders_query(port: str) -> List[dict]:
            nonlocal _orders_sql_to_use
            try:
                cursor.execute(_orders_sql_to_use, (start_compact, end_compact, port))
            except Exception:
                # Most likely: DELTA column not present.
                _orders_sql_to_use = orders_sql_base
                cursor.execute(_orders_sql_to_use, (start_compact, end_compact, port))
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, r)) for r in rows]

        option_trades: List[dict] = []
        if resolved_portfolio:
            option_trades = _run_orders_query(resolved_portfolio)

        # If exact portfolio matched 0 trades, try auto-detect using LIKE on option trades in range.
        if len(option_trades) == 0 and fund_name:
            like_patterns = _like_patterns_from_fund_name(fund_name)
            distinct_sql = (
                "SELECT DISTINCT PORTFOLIO "
                "FROM psc_filled_orders "
                "WHERE TRADE_DATE_INT BETWEEN ? AND ? "
                "AND SECURITY_TYPE = 'EquityOption' "
                "AND PORTFOLIO LIKE ? "
                "ORDER BY PORTFOLIO "
                "LIMIT 10"
            )
            for pat in like_patterns:
                cursor.execute(distinct_sql, (start_compact, end_compact, pat))
                ports = [str(r[0]).strip() for r in cursor.fetchall() if r and r[0] is not None]
                for p in ports:
                    if p and p not in portfolio_candidates:
                        portfolio_candidates.append(p)
                if portfolio_candidates:
                    break
            if portfolio_candidates:
                resolved_portfolio = portfolio_candidates[0]
                option_trades = _run_orders_query(resolved_portfolio)

        # 2) Position history (for exposures / hedge context)
        pos_sql = (
            "SELECT "
            "POSN_DATE, PORTFOLIO, STRATEGY, SECURITY, SECURITY_TYPE, SEC_CCY, SEDOL, BBG_TICKER, DESCRIPTION, SECTOR, "
            "LONG_SHORT, QUANTITY, AVG_PRICE, CLOSE_PRICE, PRICE_PROFIT, POSN_OPEN_DT, POSN_CLOSE_DT, "
            "INTEREST, DIVIDENDS, FEES, VALUE, EXPOSURE, PORTFOLIO_NAV, COUNTRY "
            "FROM psc_position_history "
            "WHERE PORTFOLIO = ? "
            "AND POSN_DATE BETWEEN ? AND ? "
            "ORDER BY PORTFOLIO, POSN_DATE, STRATEGY, SECURITY"
        )
        # Use resolved portfolio for position history too (if we found a better match)
        cursor.execute(pos_sql, (resolved_portfolio or portfolio, start_compact, end_compact))
        pos_cols = [d[0] for d in cursor.description]
        pos_rows = cursor.fetchall()
        position_history = [dict(zip(pos_cols, row)) for row in pos_rows]

        # Make sure everything is JSON-serializable
        return _json_response({
            "portfolio": resolved_portfolio or portfolio,
            "requested_portfolio": portfolio,
            "fund_name": fund_name,
            "portfolio_candidates": portfolio_candidates,
            "start_date": start_date,
            "end_date": end_date,
            "option_trades": _log_serialize(option_trades),
            "position_history": _log_serialize(position_history),
        }, 200)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Options closeout check (Desktop EMSX planned; PSC fallback available now)
# ---------------------------------------------------------------------------
def _try_import_blpapi():
    try:
        import blpapi  # type: ignore
        return blpapi, None
    except Exception as e:
        return None, str(e)


def _emsx_history_get_fills(
    date_iso: str,
    uuids: Optional[List[int]] = None,
    team: Optional[str] = None,
    host: str = "localhost",
    port: int = 8194,
    service: str = "//blp/emsx.history",
) -> List[dict]:
    """
    Pull fills via EMSX History Request service (GetFills).

    Docs: https://emsx-api-doc.readthedocs.io/en/latest/programmable/emsxHistory.html
    Note: This is a request/response API; OK for a once-per-day check around 3:50pm.
    """
    blpapi, err = _try_import_blpapi()
    if err or blpapi is None:
        raise RuntimeError(f"blpapi not available ({err}); install Bloomberg Desktop API + Python blpapi on this host")

    if not uuids and not team:
        raise RuntimeError("EMSX scope missing: set EMSX_UUIDS env var or EMSX_TEAM env var (or pass uuids/team in request)")

    from_dt = f"{date_iso}T00:00:00.000+00:00"
    to_dt = f"{date_iso}T23:59:59.999+00:00"

    # Session
    opts = blpapi.SessionOptions()
    opts.setServerHost(host)
    opts.setServerPort(port)
    session = blpapi.Session(opts)
    if not session.start():
        raise RuntimeError(f"Failed to start blpapi session to {host}:{port}")
    try:
        if not session.openService(service):
            raise RuntimeError(f"Failed to open EMSX service: {service}")
        svc = session.getService(service)
        request = svc.createRequest("GetFills")
        request.set("FromDateTime", from_dt)
        request.set("ToDateTime", to_dt)

        scope = request.getElement("Scope")
        if team:
            scope.setChoice("Team")
            scope.setElement("Team", team)
        else:
            scope.setChoice("Uuids")
            el = scope.getElement("Uuids")
            for u in uuids or []:
                el.appendValue(int(u))

        corr = session.sendRequest(request)
        fills: List[dict] = []

        while True:
            ev = session.nextEvent(5000)
            et = ev.eventType()
            for msg in ev:
                if msg.correlationIds() and msg.correlationIds()[0].value() != corr.value():
                    continue
                mtype = str(msg.messageType())
                if mtype.lower().endswith("errorinfo") or mtype == "ErrorInfo":
                    # Semi-camel casing per docs
                    code = msg.getElementAsInteger("ERROR_CODE") if msg.hasElement("ERROR_CODE") else None
                    emsg = msg.getElementAsString("ERROR_MESSAGE") if msg.hasElement("ERROR_MESSAGE") else str(msg)
                    raise RuntimeError(f"EMSX GetFills error: {code} {emsg}".strip())

                # Typical response type: GetFillsResponse
                # We avoid hard-coding element names beyond those in the docs table.
                if msg.hasElement("Fills"):
                    arr = msg.getElement("Fills")
                    for i in range(arr.numValues()):
                        f = arr.getValueAsElement(i)
                        def _get(name: str):
                            try:
                                return f.getElementAsString(name) if f.hasElement(name) else None
                            except Exception:
                                return None
                        def _get_num(name: str):
                            try:
                                return float(f.getElementAsFloat(name)) if f.hasElement(name) else None
                            except Exception:
                                try:
                                    return float(f.getElementAsString(name)) if f.hasElement(name) else None
                                except Exception:
                                    return None

                        # For options, OCCSymbol is usually the best unique identifier.
                        security = _get("OCCSymbol") or _get("SecurityName") or _get("Ticker") or ""
                        side = (_get("Side") or "").upper()

                        fills.append({
                            "SECURITY": security,
                            "TRADE_DATE_TIME": _get("DateTimeOfFill"),
                            "ORDER_ACTION": "BUY" if side == "BUY" else "SELL" if side == "SELL" else side,
                            "ACT_QTTY": _get_num("FillShares"),
                            "PRICE": _get_num("FillPrice"),
                            "BROKER": _get("Broker"),
                            "ORDER_ID": _get("OrderId"),
                            "ROUTE_ID": _get("RouteId"),
                            "TICKER": _get("Ticker"),
                            "YELLOW_KEY": _get("YellowKey"),
                        })

            if et == blpapi.Event.RESPONSE:
                break

        return fills
    finally:
        try:
            session.stop()
        except Exception:
            pass


def _options_closeout_analyze(trades: List[dict]) -> dict:
    """
    Analyze option fills for closeout mismatches / suspicious close-side usage.

    This is intentionally broker-agnostic and relies on PSC/EMSX-style order actions:
      - BUY, SELL, SELL SHORT, BUY COVR

    Returns:
      {
        open_positions: [{security, net_contracts, first_time, last_time}],
        suspicious_actions: [{security, trade_time, order_action, qty, net_before, reason}],
        by_security: {SEC: {...}}
      }
    """
    def _s(v):
        return (v or "").strip()

    def _n(v):
        try:
            return float(v)
        except Exception:
            return None

    def _trade_key(t):
        # Use whatever timestamp the source provides; keep stable ordering.
        return (_s(t.get("TRADE_DATE_INT")), _s(t.get("TRADE_DATE_TIME")), _s(t.get("ORDER_ID")))

    def _signed_delta(order_action: str, qty_abs: float) -> float:
        oa = (order_action or "").upper()
        if oa == "BUY":
            return +qty_abs
        if oa == "SELL":
            return -qty_abs
        if oa == "SELL SHORT":
            return -qty_abs
        if oa == "BUY COVR":
            return +qty_abs
        return 0.0

    grouped: Dict[str, List[dict]] = {}
    for t in trades:
        sec = _s(t.get("SECURITY"))
        if not sec:
            continue
        grouped.setdefault(sec, []).append(t)

    open_positions = []
    suspicious_actions = []
    by_security = {}

    for sec, ts in grouped.items():
        ts_sorted = sorted(ts, key=_trade_key)
        net = 0.0
        first_time = None
        last_time = None

        for t in ts_sorted:
            oa = _s(t.get("ORDER_ACTION") or t.get("ORDER") or "")
            qty = _n(t.get("ACT_QTTY")) or _n(t.get("FILLED_QTTY")) or 0.0
            qty_abs = abs(qty)
            if qty_abs == 0:
                continue

            net_before = net
            # Flag suspicious close-side usage based on current position sign.
            if net_before > 0 and oa.upper() == "SELL SHORT":
                suspicious_actions.append({
                    "security": sec,
                    "trade_time": _s(t.get("TRADE_DATE_TIME")),
                    "order_action": oa,
                    "qty": qty,
                    "net_before": net_before,
                    "reason": "Position is long; closing should typically be SELL, not SELL SHORT",
                })
            if net_before < 0 and oa.upper() == "BUY":
                suspicious_actions.append({
                    "security": sec,
                    "trade_time": _s(t.get("TRADE_DATE_TIME")),
                    "order_action": oa,
                    "qty": qty,
                    "net_before": net_before,
                    "reason": "Position is short; closing should typically be BUY COVR, not BUY",
                })

            net += _signed_delta(oa, qty_abs)
            first_time = first_time or _s(t.get("TRADE_DATE_TIME"))
            last_time = _s(t.get("TRADE_DATE_TIME")) or last_time

        by_security[sec] = {
            "net_contracts": net,
            "first_time": first_time,
            "last_time": last_time,
            "trades_count": len(ts_sorted),
        }

        # Anything still non-flat near end-of-day is a candidate for exercise risk.
        if abs(net) > 1e-9:
            open_positions.append({
                "security": sec,
                "net_contracts": net,
                "first_time": first_time,
                "last_time": last_time,
                "trades_count": len(ts_sorted),
            })

    return {
        "open_positions": open_positions,
        "suspicious_actions": suspicious_actions,
        "by_security": by_security,
    }


@app.route("/emsx/options-closeout-check", methods=["POST", "OPTIONS"])
def emsx_options_closeout_check():
    """
    Desktop EMSX closeout check (intraday).

    Uses EMSX History Request service `//blp/emsx.history` (GetFills), which is suitable for a once-per-day check.
    Docs: https://emsx-api-doc.readthedocs.io/en/latest/programmable/emsxHistory.html

    Body:
      - uuids: optional list of Bloomberg UUID ints (otherwise EMSX_UUIDS env var)
      - team: optional EMSX team name (otherwise EMSX_TEAM env var)
      - date: YYYY-MM-DD (defaults to today)
      - allow_psc_fallback: bool (default false) for non-intraday debugging only
    """
    data = request.get_json(silent=True) or {}
    # Explicitly handle CORS preflight (ngrok/browser can be strict).
    if request.method == "OPTIONS":
        return ("", 204)
    date_iso = _compact_to_ymd(data.get("date") or "") or datetime.now().strftime("%Y-%m-%d")
    try:
        allow_psc_fallback = bool(data.get("allow_psc_fallback") is True)
        uuids = data.get("uuids")
        team = (data.get("team") or "").strip() or None
        if uuids is None:
            env_uuids = os.environ.get("EMSX_UUIDS", "").strip()
            uuids = [int(x.strip()) for x in env_uuids.split(",") if x.strip()] if env_uuids else []
        else:
            uuids = [int(x) for x in (uuids or [])]
        if not team:
            team = (os.environ.get("EMSX_TEAM", "").strip() or None)

        try:
            trades = _emsx_history_get_fills(date_iso=date_iso, uuids=uuids, team=team)
            analysis = _options_closeout_analyze(_log_serialize(trades))
            return _json_response({
                "source": "emsx_history",
                "date": date_iso,
                "trade_count": len(trades),
                "scope": {"team": team, "uuids": uuids},
                **analysis,
            }, 200)
        except Exception as e:
            if not allow_psc_fallback:
                return jsonify({
                    "error": "EMSX not available on this host (required for intraday).",
                    "detail": str(e),
                    "hint": "Run DataBridge on the Bloomberg Terminal machine with Bloomberg Desktop API + Python blpapi installed and EMSX enabled; set EMSX_UUIDS or EMSX_TEAM.",
                }), 503

            # PSC fallback (debug only; can be empty intraday)
            fund_name = (data.get("fund_name") or "").strip() or None
            portfolio = _psc_portfolio_from_request(data)
            date_compact = _ymd_to_compact(date_iso)

            pyodbc, err = _pyodbc_or_503()
            if err:
                return err
            conn = pyodbc.connect("DSN=PSC_VIEWER")
            cursor = conn.cursor()
            resolved_portfolio = portfolio
            portfolio_candidates: List[str] = []
            if not resolved_portfolio and fund_name:
                like_patterns = _like_patterns_from_fund_name(fund_name)
                distinct_sql = (
                    "SELECT DISTINCT PORTFOLIO "
                    "FROM psc_filled_orders "
                    "WHERE TRADE_DATE_INT = ? "
                    "AND SECURITY_TYPE = 'EquityOption' "
                    "AND PORTFOLIO LIKE ? "
                    "ORDER BY PORTFOLIO "
                    "LIMIT 10"
                )
                for pat in like_patterns:
                    cursor.execute(distinct_sql, (date_compact, pat))
                    ports = [str(r[0]).strip() for r in cursor.fetchall() if r and r[0] is not None]
                    for p in ports:
                        if p and p not in portfolio_candidates:
                            portfolio_candidates.append(p)
                    if portfolio_candidates:
                        break
                if portfolio_candidates:
                    resolved_portfolio = portfolio_candidates[0]
            if not resolved_portfolio:
                return jsonify({"error": "portfolio required for PSC fallback"}), 400
            fills_sql = (
                "SELECT "
                "ORDER_ID, PORTFOLIO, SECURITY, DESCRIPTION, SECURITY_TYPE, "
                "TRADE_DATE_INT, TRADE_DATE_TIME, `ORDER` AS ORDER_ACTION, "
                "ACT_QTTY, FILLED_QTTY, PRICE, BROKER "
                "FROM psc_filled_orders "
                "WHERE TRADE_DATE_INT = ? "
                "AND PORTFOLIO = ? "
                "AND SECURITY_TYPE = 'EquityOption' "
                "ORDER BY SECURITY, TRADE_DATE_TIME, ORDER_ID"
            )
            cursor.execute(fills_sql, (date_compact, resolved_portfolio))
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            psc_trades = [dict(zip(cols, r)) for r in rows]
            analysis = _options_closeout_analyze(_log_serialize(psc_trades))
            return _json_response({
                "source": "psc_fallback",
                "date": date_iso,
                "portfolio": resolved_portfolio,
                "requested_portfolio": portfolio,
                "portfolio_candidates": portfolio_candidates,
                "trade_count": len(psc_trades),
                "emsx_error": str(e),
                **analysis,
            }, 200)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def _get_diamond_fund_ids():
    """Return list of fund IDs from env. SGGG_DIAMOND_FUND_IDS (comma-separated) or SGGG_DIAMOND_FUND_ID (single)."""
    ids_str = os.environ.get("SGGG_DIAMOND_FUND_IDS", "").strip()
    if ids_str:
        return [x.strip() for x in ids_str.split(",") if x.strip()]
    single = os.environ.get("SGGG_DIAMOND_FUND_ID", "").strip()
    if single:
        return [single]
    return []


def _get_default_fund_id():
    """Return first fund ID for single-request endpoints."""
    ids = _get_diamond_fund_ids()
    return ids[0] if ids else None


# ---------------------------------------------------------------------------
# SGGG Diamond API endpoints (runs in parallel with PSC/ODBC)
# Requires: SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD
# Fund IDs: SGGG_DIAMOND_FUND_IDS (comma-separated) or SGGG_DIAMOND_FUND_ID (single)
# ---------------------------------------------------------------------------
@app.route("/sggg/diamond/health", methods=["GET"])
def sggg_diamond_health():
    """Test SGGG Diamond API (HTTP) connection: login to api.sgggfsi.com."""
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"ok": False, "error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD."}), 503
        client._ensure_auth()
        return jsonify({"ok": True, "connection": "diamond_api", "base_url": "https://api.sgggfsi.com/api/v1"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/sggg/diamond/portfolio", methods=["GET", "POST"])
def sggg_diamond_portfolio():
    """
    Get finalized portfolio from SGGG Diamond API.
    Body or query: fund_id (optional), all=true (fetch all configured funds), valuation_date (yyyy-mm-dd)
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD, SGGG_DIAMOND_FUND_IDS."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = request.args.get("fund_id") or data.get("fund_id")
        fetch_all = request.args.get("all", "").lower() in ("1", "true", "yes") or data.get("all") is True
        valuation_date = request.args.get("date") or request.args.get("valuation_date") or data.get("date") or data.get("valuation_date")
        if not valuation_date:
            valuation_date = datetime.now().strftime("%Y-%m-%d")
        else:
            valuation_date = valuation_date.replace("-", "")[:8]
            valuation_date = f"{valuation_date[:4]}-{valuation_date[4:6]}-{valuation_date[6:8]}"
        fund_ids = _get_diamond_fund_ids() if fetch_all else ([fund_id] if fund_id else [_get_default_fund_id()])
        if not fund_ids or not fund_ids[0]:
            return jsonify({"error": "fund_id required, or set SGGG_DIAMOND_FUND_IDS (comma-separated) and use all=true"}), 400
        if fetch_all and len(fund_ids) > 1:
            results = {}
            for fid in fund_ids:
                try:
                    results[fid] = client.get_portfolio(fund_id=fid, valuation_date=valuation_date)
                except Exception as e:
                    results[fid] = {"error": str(e)}
            return jsonify({"funds": results, "valuation_date": valuation_date})
        result = client.get_portfolio(fund_id=fund_ids[0], valuation_date=valuation_date)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/sggg/diamond/trades", methods=["GET", "POST"])
def sggg_diamond_trades():
    """
    Get portfolio trades from SGGG Diamond API.
    Body or query: fund_id (optional), all=true (fetch all), start_date, end_date (yyyy-mm-dd, max 1 month range)
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = request.args.get("fund_id") or data.get("fund_id")
        fetch_all = request.args.get("all", "").lower() in ("1", "true", "yes") or data.get("all") is True
        start_date = request.args.get("start_date") or data.get("start_date")
        end_date = request.args.get("end_date") or data.get("end_date")
        fund_ids = _get_diamond_fund_ids() if fetch_all else ([fund_id] if fund_id else [_get_default_fund_id()])
        if not fund_ids or not fund_ids[0]:
            return jsonify({"error": "fund_id required, or set SGGG_DIAMOND_FUND_IDS and use all=true"}), 400
        if fetch_all and len(fund_ids) > 1:
            results = {}
            for fid in fund_ids:
                try:
                    results[fid] = client.get_portfolio_trades(
                        fund_parent_id=fid, start_date=start_date, end_date=end_date
                    )
                except Exception as e:
                    results[fid] = {"error": str(e)}
            return jsonify({"funds": results})
        result = client.get_portfolio_trades(
            fund_parent_id=fund_ids[0],
            start_date=start_date,
            end_date=end_date,
        )
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def _refdata_result_summary(ref: Dict[str, Any]) -> tuple[int, int]:
    """Return (num_tickers, num_with_error_key) for a get_reference_data result dict."""
    if not ref:
        return 0, 0
    n_err = sum(1 for v in ref.values() if isinstance(v, dict) and "error" in v)
    return len(ref), n_err


def _eco_calendar_parse_date(release_dt: Any) -> Optional[date_type]:
    """Parse ECO_RELEASE_DT / ECO_FUTURE_RELEASE_DATE refdata values to a date."""
    if not release_dt:
        return None
    if isinstance(release_dt, datetime):
        return release_dt.date()
    if isinstance(release_dt, date_type):
        return release_dt
    if isinstance(release_dt, str):
        try:
            s = release_dt.strip()
            if len(s) >= 10 and s[4] == "-":
                return datetime.strptime(s[:10], "%Y-%m-%d").date()
            if len(s) >= 8:
                return datetime.strptime(s[:8], "%Y%m%d").date()
        except Exception:
            return None
    return None


@app.route("/economic-calendar", methods=["POST"])
def economic_calendar():
    """
    Fetch economic calendar data for given tickers or all configured tickers.
    Uses BDP (Bloomberg Data Point) to get current/future release data.
    """
    try:
        data = request.get_json(silent=True, force=True) or {}
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
            return _json_response(
                {
                    "success": False,
                    "error": "No tickers provided and no tickers configured. Please provide tickers in request body or import economic_calendar_tickers.sql",
                    "calendar_data": [],
                },
                400,
            )
        
        # Date range: today to today + 365 days
        today = datetime.now().date()
        end_date = today + timedelta(days=365)
        today_str = today.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        _ecal_logger.info(
            "request_start build=%s tickers=%d date_range=%s..%s",
            DATA_BRIDGE_BUILD,
            len(tickers),
            today_str,
            end_date_str,
        )
        verbose = os.environ.get("ECONOMIC_CALENDAR_LOG_VERBOSE", "").lower() in (
            "1",
            "true",
            "yes",
        )
        if verbose:
            preview = tickers[:20]
            more = "" if len(tickers) <= 20 else f" ...(+{len(tickers) - 20} more)"
            _ecal_logger.info("request_tickers_preview=%s%s", preview, more)
        print(
            f"[economic-calendar] tickers_count={len(tickers)} date_range=[{today_str}, {end_date_str}]",
            flush=True,
        )
        
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
        _ecal_logger.info("refdata_fields n=%d %s", len(fields), fields)
        
        # For future dates, we need to use ECO_FUTURE_RELEASE_DATE
        # For current/past dates, we use the regular fields
        
        events = []
        errors = []

        # One Bloomberg session for the whole request (many refdata calls); avoids flaky multi-connect
        bb_sess = bloomberg_client.open_refdata_session()
        if not bb_sess:
            _ecal_logger.error("bloomberg_session_open_failed")
            return _json_response(
                {
                    "success": False,
                    "error": "Failed to start Bloomberg session. Is Bloomberg Terminal running and logged in?",
                    "calendar_data": [],
                },
                500,
            )

        try:
            # Process tickers in batches to avoid overwhelming Bloomberg
            BATCH_SIZE = 50
            for i in range(0, len(tickers), BATCH_SIZE):
                batch_tickers = tickers[i:i+BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                _ecal_logger.info(
                    "blp_refdata_call batch=%d tickers=%d fields=%d",
                    batch_num,
                    len(batch_tickers),
                    len(fields),
                )
                if verbose:
                    _ecal_logger.info(
                        "blp_refdata_call batch=%d tickers_list=%s",
                        batch_num,
                        batch_tickers,
                    )
                print(
                    f"Processing batch {batch_num} ({len(batch_tickers)} tickers)...",
                    flush=True,
                )
            
                try:
                    # Fetch reference data for this batch
                    reference_data = bloomberg_client.get_reference_data(
                        tickers=batch_tickers,
                        fields=fields,
                        session=bb_sess,
                    )
                    got, n_err = _refdata_result_summary(reference_data)
                    _ecal_logger.info(
                        "blp_refdata_response batch=%d rows=%d rows_with_error=%d",
                        batch_num,
                        got,
                        n_err,
                    )
                    log_resp = os.environ.get(
                        "ECONOMIC_CALENDAR_LOG_RESPONSE", ""
                    ).lower() in ("1", "true", "yes")
                    if log_resp or os.environ.get("ECONOMIC_CALENDAR_DEBUG_FULL", "").lower() in (
                        "1",
                        "true",
                        "yes",
                    ):
                        try:
                            _serialized = _log_serialize(reference_data)
                            blob = json.dumps(_serialized, default=str)
                            max_len = int(
                                os.environ.get("ECONOMIC_CALENDAR_LOG_RESPONSE_MAX", "12000")
                            )
                            if len(blob) > max_len:
                                blob = blob[:max_len] + f"...(truncated, len={len(blob)})"
                            _ecal_logger.info(
                                "blp_refdata_response_body batch=%d json=%s",
                                batch_num,
                                blob,
                            )
                        except Exception as log_err:
                            _ecal_logger.warning(
                                "blp_refdata_response_body_log_failed batch=%d err=%s",
                                batch_num,
                                log_err,
                            )

                    # Phase 1: ECO_RELEASE_DT only (no per-ticker Bloomberg round-trips)
                    batch_states: List[dict] = []
                    for ticker, data in reference_data.items():
                        if "error" in data:
                            errors.append(f"{ticker}: {data['error']}")
                            continue
                        eco_rd = _eco_calendar_parse_date(data.get("ECO_RELEASE_DT"))
                        batch_states.append(
                            {"ticker": ticker, "data": data, "release_date": eco_rd}
                        )

                    # Phase 2: one batched refdata call per chunk for missing dates only
                    need_future = [s["ticker"] for s in batch_states if s["release_date"] is None]
                    future_by_ticker: Dict[str, dict] = {}
                    if need_future:
                        for fj in range(0, len(need_future), BATCH_SIZE):
                            chunk = need_future[fj : fj + BATCH_SIZE]
                            fut_bn = fj // BATCH_SIZE + 1
                            _ecal_logger.info(
                                "blp_future_call batch=%d chunk=%d tickers=%d fields=ECO_FUTURE_RELEASE_DATE",
                                batch_num,
                                fut_bn,
                                len(chunk),
                            )
                            try:
                                fr_batch = bloomberg_client.get_reference_data(
                                    tickers=chunk,
                                    fields=["ECO_FUTURE_RELEASE_DATE"],
                                    session=bb_sess,
                                )
                                fg, fe_ct = _refdata_result_summary(fr_batch)
                                _ecal_logger.info(
                                    "blp_future_response batch=%d chunk=%d rows=%d rows_with_error=%d",
                                    batch_num,
                                    fut_bn,
                                    fg,
                                    fe_ct,
                                )
                                if log_resp:
                                    try:
                                        ser = _log_serialize(fr_batch)
                                        b = json.dumps(ser, default=str)
                                        mx = int(
                                            os.environ.get(
                                                "ECONOMIC_CALENDAR_LOG_RESPONSE_MAX", "12000"
                                            )
                                        )
                                        if len(b) > mx:
                                            b = b[:mx] + f"...(truncated len={len(b)})"
                                        _ecal_logger.info(
                                            "blp_future_response_body batch=%d chunk=%d json=%s",
                                            batch_num,
                                            fut_bn,
                                            b,
                                        )
                                    except Exception as le:
                                        _ecal_logger.warning(
                                            "blp_future_response_body_log_failed err=%s", le
                                        )
                                future_by_ticker.update(fr_batch)
                            except Exception as fe:
                                _ecal_logger.error(
                                    "blp_future_exception batch=%d chunk=%s: %s",
                                    batch_num,
                                    fut_bn,
                                    fe,
                                    exc_info=True,
                                )
                                errors.append(
                                    f"ECO_FUTURE_RELEASE_DATE batch {fj // BATCH_SIZE + 1}: {fe}"
                                )

                    # Phase 3: merge future dates and build events
                    for s in batch_states:
                        ticker = s["ticker"]
                        data = s["data"]
                        release_date = s["release_date"]
                        release_time = None
                        is_future_release = False

                        if release_date is None:
                            fr_row = future_by_ticker.get(ticker, {})
                            if "error" not in fr_row:
                                future_release_date = _eco_calendar_parse_date(
                                    fr_row.get("ECO_FUTURE_RELEASE_DATE")
                                )
                                if future_release_date:
                                    release_date = future_release_date
                                    is_future_release = True

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
                    
                        # Future event only when release_date is strictly after today. For today or past, set actual from PX_LAST.
                        is_future_event = False
                        if is_future_release and release_date and release_date > today:
                            is_future_event = True
                        elif release_date and release_date > today:
                            is_future_event = True
                        # Removed time-of-day check for today: it caused actual to be null when server timezone was before release time.
                        if False and release_date == today and release_time:
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
                    
                        # Build event record (plain str/float/None only — safe for JSON)
                        event = {
                            "ticker": str(ticker),
                            "country": _json_scalar_str(data.get("REGION_OR_COUNTRY")),
                            "event": _json_scalar_str(data.get("SECURITY_DES")),
                            "release_date": release_date.strftime("%Y-%m-%d") if release_date else None,
                            "release_time": release_time,
                            "period": _json_scalar_str(data.get("OBSERVATION_PERIOD")),
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
                    _ecal_logger.error(
                        "batch_processing_failed batch=%d: %s",
                        batch_num,
                        e,
                        exc_info=True,
                    )
                    print(f"  [FAIL] {error_msg}", flush=True)
                    errors.append(error_msg)
                    traceback.print_exc(file=sys.stderr)
                    sys.stderr.flush()
        finally:
            try:
                bb_sess.stop()
            except Exception:
                pass

        _ecal_logger.info(
            "request_done events=%d errors=%d",
            len(events),
            len(errors),
        )
        print(f"Fetched {len(events)} economic calendar events", flush=True)
        if len(events) == 0:
            _ecal_logger.warning(
                "zero_events_after_filter today=%s end_date=%s",
                today_str,
                end_date_str,
            )
            print(
                f"[economic-calendar] ZERO EVENTS: date filter today={today!r} end_date={end_date!r}",
                flush=True,
            )
        
        # Return calendar_data to match Edge Function expectations
        # Also include events for backward compatibility
        return _json_response(
            {
                "success": True,
                "service": "DataBridge",
                "calendar_data": events,
                "events": events,
                "count": len(events),
                "errors": errors,
                "date_range": {"from": today_str, "to": end_date_str},
            }
        )

    except Exception as e:
        error_msg = f"Economic calendar error: {str(e)}"
        _ecal_logger.error("route_outer_exception: %s", e, exc_info=True)
        print(f"[FAIL] {error_msg}", flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        return _json_response(
            {"success": False, "service": "DataBridge", "error": error_msg, "calendar_data": []},
            500,
        )


def _safe_float(value):
    """Safely convert value to float, return None if not possible (JSON cannot encode inf/nan)."""
    if value is None:
        return None
    try:
        x = float(value)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except (ValueError, TypeError):
        return None


def _parse_date(value):
    """Parse Bloomberg date value to YYYY-MM-DD string"""
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.date().strftime("%Y-%m-%d")
        if isinstance(value, date_type):
            return value.strftime("%Y-%m-%d")
        elif isinstance(value, str):
            # Try parsing various formats
            if len(value) >= 8:
                return datetime.strptime(value[:8], "%Y%m%d").date().strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def _log_serialize(obj):
    """Convert obj to a JSON-friendly form for console logging (dates/times to string)."""
    if obj is None:
        return None
    if isinstance(obj, (date_type, datetime)):
        return obj.isoformat() if hasattr(obj, "isoformat") else str(obj)
    if isinstance(obj, dt_time):
        return obj.strftime("%H:%M:%S") if hasattr(obj, "strftime") else str(obj)
    if isinstance(obj, dict):
        return {k: _log_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_log_serialize(v) for v in obj]
    return obj


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


def _ibkr_request(
    method: str,
    path: str,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> tuple[Optional[requests.Response], Optional[Dict[str, Any]]]:
    """Call IBKR Gateway with 10 req/s rate limit. Returns (response, None) or (None, error_dict)."""
    global _ibkr_last_request_time
    with _ibkr_rate_lock:
        now = time.monotonic()
        elapsed = now - _ibkr_last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        _ibkr_last_request_time = time.monotonic()
    try:
        url = f"{IBKR_GATEWAY_URL}{path}" if path.startswith("/") else f"{IBKR_GATEWAY_URL}/{path}"
        if method.upper() == "GET":
            r = _ibkr_session.get(url, params=params, timeout=timeout)
        else:
            r = _ibkr_session.post(url, json=json_body or {}, params=params, timeout=timeout)
        return (r, None)
    except requests.exceptions.RequestException as e:
        return (None, {"error": "IBKR Gateway unreachable", "detail": str(e)})
    except Exception as e:
        return (None, {"error": "IBKR request failed", "detail": str(e)})


def _ibkr_response_json(r: requests.Response) -> Any:
    """Parse Gateway response as JSON; on failure return status and raw body for debugging."""
    try:
        return r.json()
    except Exception:
        return {"_gateway_status": r.status_code, "_gateway_body": (r.text or "(empty)")[:2000]}


@app.route("/ibkr/auth-status", methods=["GET"])
def ibkr_auth_status():
    """Proxy to IBKR Gateway auth status (POST /iserver/auth/status)."""
    r, err = _ibkr_request("POST", "/v1/api/iserver/auth/status", json_body={}, timeout=10)
    if err:
        return jsonify(err), 502
    data = _ibkr_response_json(r)
    return jsonify(data), r.status_code


@app.route("/ibkr/snapshot", methods=["GET"])
def ibkr_snapshot():
    """Proxy to IBKR Gateway market data snapshot. Query: conids (required), fields (required)."""
    conids = request.args.get("conids")
    fields = request.args.get("fields")
    if not conids or not fields:
        return jsonify({"error": "conids and fields are required"}), 400
    params = {"conids": conids, "fields": fields}
    r, err = _ibkr_request("GET", "/v1/api/iserver/marketdata/snapshot", params=params, timeout=15)
    if err:
        return jsonify(err), 502
    data = _ibkr_response_json(r)
    return (jsonify(data) if isinstance(data, (dict, list)) else jsonify({"raw": data})), r.status_code


@app.route("/ibkr/history", methods=["GET"])
def ibkr_history():
    """Proxy to IBKR Gateway historical market data. Query: conid, period, bar (required); exchange, startTime, outsideRth, source (optional). Max 5 concurrent."""
    conid = request.args.get("conid")
    period = request.args.get("period")
    bar = request.args.get("bar")
    if not conid or not period or not bar:
        return jsonify({"error": "conid, period, and bar are required"}), 400
    params = {"conid": conid, "period": period, "bar": bar}
    for key in ("exchange", "startTime", "outsideRth", "source"):
        val = request.args.get(key)
        if val is not None:
            params[key] = val
    _ibkr_history_semaphore.acquire()
    try:
        r, err = _ibkr_request("GET", "/v1/api/iserver/marketdata/history", params=params, timeout=30)
        if err:
            return jsonify(err), 502
        data = _ibkr_response_json(r)
        return (jsonify(data) if isinstance(data, (dict, list)) else jsonify({"raw": data})), r.status_code
    finally:
        _ibkr_history_semaphore.release()


@app.route("/ibkr/search", methods=["GET"])
def ibkr_search():
    """Proxy to IBKR Gateway symbol search. Query: symbol (required); name, secType (optional)."""
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400
    params = {"symbol": symbol}
    for key in ("name", "secType"):
        val = request.args.get(key)
        if val is not None:
            params[key] = val
    r, err = _ibkr_request("GET", "/v1/api/iserver/secdef/search", params=params, timeout=10)
    if err:
        return jsonify(err), 502
    data = _ibkr_response_json(r)
    return (jsonify(data) if isinstance(data, (dict, list)) else jsonify({"raw": data})), r.status_code


POLYMARKET_ALERT_WEBHOOK_SECRET = os.environ.get("POLYMARKET_ALERT_WEBHOOK_SECRET", "").strip()


def _parse_optional_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@app.route("/polymarket/alert", methods=["POST"])
def polymarket_alert():
    """Ingest Polymarket insider alert into Supabase (service role). Bearer auth required."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 503
    if not POLYMARKET_ALERT_WEBHOOK_SECRET:
        return jsonify({"error": "POLYMARKET_ALERT_WEBHOOK_SECRET not set"}), 503
    auth = request.headers.get("Authorization") or ""
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    if not secrets.compare_digest(token, POLYMARKET_ALERT_WEBHOOK_SECRET):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "JSON body required"}), 400

    dedup_key = (data.get("dedup_key") or "").strip()
    wallet_address = (data.get("wallet_address") or "").strip()
    market_id = (data.get("market_id") or "").strip()
    if not dedup_key or not wallet_address or not market_id:
        return jsonify({"error": "dedup_key, wallet_address, and market_id are required"}), 400

    trade_price = _parse_optional_float(data.get("trade_price"))
    trade_size_usdc = _parse_optional_float(data.get("trade_size_usdc"))

    signals = data.get("signals_triggered")
    if not isinstance(signals, list):
        signals = []
    signals = [str(s) for s in signals if s is not None]

    risk_score = data.get("risk_score")
    try:
        risk_score_f = float(risk_score) if risk_score is not None else 0.0
    except (TypeError, ValueError):
        risk_score_f = 0.0

    row: Dict[str, Any] = {
        "dedup_key": dedup_key,
        "wallet_address": wallet_address,
        "market_id": market_id,
        "trade_id": data.get("trade_id"),
        "trade_side": data.get("trade_side"),
        "trade_price": trade_price,
        "trade_size_usdc": trade_size_usdc,
        "risk_score": risk_score_f,
        "signals_triggered": signals,
        "assessment_id": data.get("assessment_id"),
        "payload": data.get("payload") if isinstance(data.get("payload"), dict) else data,
    }
    if data.get("alerted_at"):
        row["alerted_at"] = data["alerted_at"]

    try:
        supabase.table("at_polymarket_alerts").insert(row).execute()
    except Exception as e:
        err_s = str(e).lower()
        if "duplicate" in err_s or "23505" in err_s or "unique" in err_s:
            return jsonify({"ok": True, "duplicate": True}), 200
        logging.exception("polymarket alert insert failed")
        return jsonify({"error": "insert failed", "detail": str(e)[:500]}), 500

    return jsonify({"ok": True}), 200


def _ibkr_tickle_loop() -> None:
    """Background thread: POST /tickle to IBKR Gateway every 60s to keep session alive."""
    while True:
        time.sleep(60)
        try:
            r = requests.post(
                f"{IBKR_GATEWAY_URL.rstrip('/')}/v1/api/tickle",
                json={},
                timeout=5,
                verify=False,
            )
            if os.environ.get("DATA_BRIDGE_DEBUG") and r.status_code == 200:
                print("[IBKR] tickle OK")
        except requests.exceptions.RequestException:
            pass  # Gateway not running or not logged in; ignore
        except Exception:
            pass


if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True)
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass
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
        print("  /economic-calendar, /clarifi/process, /clarifi/list, /ehp/process, /sggg/portfolio,")
        print("  /sggg/options-tax-reconciliation,")
        print("  /emsx/options-closeout-check,")
        print("  /ibkr/auth-status, /ibkr/snapshot, /ibkr/history, /ibkr/search, /polymarket/alert")
        print("SGGG requires: OpenVPN + DSN=PSC_VIEWER + pyodbc. IBKR requires Gateway on port 5001 + browser login.")
        print()
        print("Service is running. Press Ctrl+C to stop.")
        print(
            "Economic calendar logs: stderr [ecal] — set ECONOMIC_CALENDAR_LOG_VERBOSE=1 for ticker lists, "
            "ECONOMIC_CALENDAR_LOG_RESPONSE=1 for truncated Bloomberg JSON (max ECONOMIC_CALENDAR_LOG_RESPONSE_MAX)."
        )
        print(
            "Market Bloomberg (/historical, /reference): stderr [bbg] summaries; "
            "DATA_BRIDGE_BLOOMBERG_VERBOSE=1 or DATA_BRIDGE_DEBUG=1 for full JSON bodies."
        )
        print(
            f"Canonical dashboard bridge: this DataBridge on port {SERVICE_PORT} (set PORT= to override). "
            "Point ngrok/localtunnel at this port; Supabase DATA_BRIDGE_URL must reach this process—not bloomberg-local-service.py:8765."
        )
        print("Tip: run with python -u or PYTHONUNBUFFERED=1 so prints appear immediately.")
        print("=" * 60)
    except:
        pass

    # Keep IBKR Gateway session alive (tickle every 60s)
    _tickle_thread = threading.Thread(target=_ibkr_tickle_loop, daemon=True)
    _tickle_thread.start()

    try:
        app.run(host="127.0.0.1", port=SERVICE_PORT, debug=False, use_reloader=False)
    except Exception as e:
        print(f"Error starting service: {e}")
        traceback.print_exc()
        sys.exit(1)

