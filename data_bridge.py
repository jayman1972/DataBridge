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
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import socket
import subprocess
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
from sggg.diamond_client import (
    DiamondNavUnavailableError,
    fetch_nav_sheet,
    get_diamond_client,
    fund_aum_from_summary,
    get_nav_sheet_raw_cached,
    nav_sheet_summary_cacheable,
    set_nav_sheet_raw_cached,
    should_skip_diamond_early_for_today,
)
from sggg.nav_sheet_parse import (
    FUND_NATIVE_CURRENCY,
    capital_flow_net_from_summary,
    fetch_psc_portfolio_navs,
    normalize_diamond_sheet_date,
    normalize_valuation_date,
    parse_nav_sheet_summary,
    prior_business_day_iso,
    prior_business_days_for_lookup,
    prior_open_sheet_is_usable,
    enrich_classes_display_labels,
    pick_class_i_bps,
    sggg_opening_aum_from_prior_summary,
)
from sggg.compliance_check_estimates import compliance_aum_change_ex_flows, estimates_by_fund_id
from sggg.diamond_nav_store import load_snapshots_bulk, snapshot_usable, upsert_snapshot
from sggg.psc_boxed_positions import fetch_boxed_positions_for_funds
from sggg.close_price_reconcile import fetch_close_price_reconciliation

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
    "41010000-7F7A-0A65-D559-45484608DB40": "EHP Tact Growth Alt",
    "41323030-3031-4144-3637-303030364338": "EHP Select Alt",
    "41010000-7F2A-D7E8-776F-45484608D91C": "EHP Strat Inc Alt",
    "01010000-801A-4995-8370-45484608DE57": "Expon Bal Grow Fund",
}

# "All funds" for closeout checks should include Alpha even if it's not in the fund-id mapping.
PSC_ALL_FUNDS_PORTFOLIOS: List[str] = [
    "EHP Alpha",
    "EHP Select Alt",
    "EHP Strat Inc Alt",
    "EHP Tact Growth Alt",
]

# Cache PSC start-of-day option positions by (asof_date, portfolios_key).
# This avoids repeatedly hitting PSC for the same snapshot when you rerun the report.
_PSC_START_POS_CACHE: Dict[str, Dict[str, float]] = {}
_PSC_START_POS_CACHE_META: Dict[str, dict] = {}


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


def _psc_probe_snapshot_before(cursor, portfolio: str, report_compact: str, lookback_days: int = 14) -> Optional[str]:
    """
    Resolve latest POSN_DATE strictly before report_compact by probing likely dates with equality.
    This avoids expensive MAX()/ORDER BY scans which can time out on some PSC installs.
    """
    try:
        report_date = datetime(int(report_compact[:4]), int(report_compact[4:6]), int(report_compact[6:8]))
    except Exception:
        return None

    for back in range(1, max(1, int(lookback_days)) + 1):
        cand = report_date - timedelta(days=back)
        cand_compact = cand.strftime("%Y%m%d")
        cursor.execute(
            "SELECT 1 FROM psc_position_history WHERE PORTFOLIO = ? AND POSN_DATE = ? LIMIT 1",
            (portfolio, cand_compact),
        )
        r = cursor.fetchone()
        if r:
            return cand_compact

    return None


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
    """Bloomberg historical data - format: { symbols, fields, start_date?, end_date?, overrides? }."""
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

        overrides = data.get("overrides")
        if overrides is not None and not isinstance(overrides, dict):
            return jsonify({"error": "overrides must be an object/dict"}), 400

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
                        overrides=overrides,
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
    """Bloomberg reference/EOD data - format: { symbols, fields, overrides? }. Returns { reference_data: { TICKER: [{date, ...}] } }."""
    try:
        data = request.get_json() or {}
        symbols = data.get("symbols") or []
        fields = data.get("fields") or []
        overrides = data.get("overrides") or None
        if not symbols or not fields:
            return jsonify({"error": "symbols and fields are required"}), 400
        if overrides is not None and not isinstance(overrides, dict):
            return jsonify({"error": "overrides must be an object/dict"}), 400

        print(f"[DataBridge reference] REQUEST (full): symbols={symbols} fields={fields}", flush=True)
        _bbg_logger.info(
            "reference REQUEST symbols=%d fields=%s sample=%s",
            len(symbols),
            fields,
            symbols[:12] if len(symbols) > 12 else symbols,
        )
        ref_data = bloomberg_client.get_reference_data(tickers=symbols, fields=fields, overrides=overrides)
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
        # Prefer query param `fund=` (from the app), but be defensive: some proxies/clients
        # have been observed to drop non-date params from request.args. Fall back to parsing
        # the raw query string and/or JSON body.
        fund = (request.args.get("fund") or "").strip()
        if not fund:
            try:
                from urllib.parse import parse_qs
                qs = parse_qs((request.query_string or b"").decode("utf-8", errors="ignore"))
                fund = (qs.get("fund", [""])[0] or "").strip()
            except Exception:
                fund = ""
        if not fund:
            fund = ((data.get("fund") or data.get("fund_name") or "")).strip()
        if not fund:
            fund = "EHP Select Alt"
        if query_date:
            query_date = query_date.replace("-", "")[:8]
        else:
            query_date = datetime.now().strftime("%Y%m%d")

        print(f"[/sggg/portfolio] date={query_date} fund={fund}", flush=True)

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

            # IMPORTANT: For external reporting, do NOT filter by STRATEGY or SECURITY_TYPE.
            # Strategies/security-type whitelists are internal and can cause valid portfolios
            # (e.g. credit/income) to return empty.
            #
            # Use POSN_DATE_INT (YYYYMMDD) to match VBA usage and avoid POSN_DATE type differences.
            #
            # Some PSC portfolios have minor naming variants (suffixes, spacing). Prefer an exact match,
            # and only fall back to a prefix LIKE when the exact match returns zero rows. This avoids
            # ambiguous matches like "EHP Alpha" accidentally pulling "EHP Alpha Hedge".
            sql_base = (
                "SELECT "
                "  ph.STRATEGY, ph.TRADE_GROUP, ph.COMPANY_SYMBOL, ph.DESCRIPTION, ph.SECURITY_TYPE, "
                "  ph.SEC_CCY AS Currency, ph.BBG_TICKER, ph.SECTOR, ph.COUNTRY, ph.LONG_SHORT, "
                "  sd.SEDOL, "
            )
            sql_option_extra = (
                "  MAX(sd.STRIKE) AS STRIKE, "
                "  MAX(ph.SECURITY_DELTA) AS SECURITY_DELTA, "
            )
            sql_tail_from = (
                "FROM psc_position_history ph "
                "LEFT JOIN psc_security_data sd ON ph.security_sn = sd.security_sn "
                "WHERE (ph.PORTFOLIO = ? OR ph.PORTFOLIO LIKE ?) AND ph.POSN_DATE_INT = ? "
                "GROUP BY "
                "  ph.STRATEGY, ph.TRADE_GROUP, ph.COMPANY_SYMBOL, ph.DESCRIPTION, ph.SECURITY_TYPE, "
                "  ph.SEC_CCY, ph.BBG_TICKER, ph.SECTOR, ph.COUNTRY, ph.LONG_SHORT, sd.SEDOL "
                "ORDER BY ph.STRATEGY, ph.TRADE_GROUP, ph.COMPANY_SYMBOL"
            )
            sql_tail_core = (
                "  SUM(ph.QUANTITY) AS QUANTITY, "
                "  AVG(ph.AVG_PRICE) AS AVG_PRICE, "
                "  MAX(ph.CLOSE_PRICE) AS CLOSE_PRICE, "
                "  SUM(ph.PRICE_PROFIT) AS PRICE_PROFIT, "
                "  MAX(ph.FX_SETTLE_TO_BASE) AS FX_SETTLE_TO_BASE, "
                "  SUM(ph.INTEREST) AS INTEREST, "
                "  SUM(ph.DIVIDENDS) AS DIVIDENDS, "
                "  SUM(ph.VALUE) AS VALUE, "
                "  SUM(ph.EXPOSURE) AS EXPOSURE, "
                "  SUM(ph.DAY_PROFIT) AS DAY_PROFIT, "
                "  MAX(ph.PORTFOLIO_NAV) AS PORTFOLIO_NAV "
            )

            def _psc_metrics_fragment(mode: str) -> str:
                # AlphaDesk columns: EXPOSURE PCT NAV, FX EXPOSURE PCT NAV, BETA PCT NAV
                if mode == "pct_full":
                    return (
                        "  SUM(ph.EXPOSURE_PCT_NAV) AS EXPOSURE_PCT_NAV, "
                        "  SUM(ph.FX_EXPOSURE_PCT_NAV) AS FX_EXPOSURE_PCT_NAV, "
                        "  SUM(ph.BETA_PCT_NAV) AS BETA_PCT_NAV, "
                    )
                if mode == "pct_fx_beta":
                    return (
                        "  SUM(ph.FX_EXPOSURE_PCT_NAV) AS FX_EXPOSURE_PCT_NAV, "
                        "  SUM(ph.BETA_PCT_NAV) AS BETA_PCT_NAV, "
                    )
                if mode == "loc_beta":
                    return (
                        "  SUM(ph.FX_EXPOSURE_LOC) AS FX_EXPOSURE_LOC, "
                        "  SUM(ph.BETA_PCT_NAV) AS BETA_PCT_NAV, "
                    )
                return ""

            def _build_portfolio_sql(metrics_mode: str, with_options: bool) -> str:
                metrics = _psc_metrics_fragment(metrics_mode)
                body = sql_base + (sql_option_extra if with_options else "") + metrics + sql_tail_core + sql_tail_from
                return body

            def _run_portfolio_query(sql_text: str, params: tuple):
                cursor.execute(sql_text, params)
                return cursor.fetchall()

            def _psc_pct_nav_fraction(v):
                n = _num(v)
                if n is None:
                    return None
                # PSC / AlphaDesk often stores 32.68 meaning 32.68%, not 0.3268
                if abs(n) > 3:
                    return n / 100.0
                return n

            rows = []
            has_option_columns = False
            metrics_mode = "none"
            metrics_modes_to_try = ["pct_full", "pct_fx_beta", "loc_beta", "none"]

            for metrics_mode_try in metrics_modes_to_try:
                for with_options in (True, False):
                    candidate_sql = _build_portfolio_sql(metrics_mode_try, with_options)
                    try:
                        sql_exact = candidate_sql.replace(
                            "WHERE (ph.PORTFOLIO = ? OR ph.PORTFOLIO LIKE ?) AND ph.POSN_DATE_INT = ? ",
                            "WHERE ph.PORTFOLIO = ? AND ph.POSN_DATE_INT = ? ",
                        )
                        rows = _run_portfolio_query(sql_exact, (fund, query_date))
                        if not rows:
                            sql_like = candidate_sql.replace(
                                "WHERE (ph.PORTFOLIO = ? OR ph.PORTFOLIO LIKE ?) AND ph.POSN_DATE_INT = ? ",
                                "WHERE ph.PORTFOLIO LIKE ? AND ph.POSN_DATE_INT = ? ",
                            )
                            rows = _run_portfolio_query(sql_like, (f"{fund}%", query_date))
                        if rows:
                            has_option_columns = with_options
                            metrics_mode = metrics_mode_try
                            print(
                                f"[/sggg/portfolio] metrics_mode={metrics_mode} options={with_options} rows={len(rows)}",
                                flush=True,
                            )
                            break
                    except Exception as err:
                        if with_options:
                            print(
                                f"[/sggg/portfolio] query failed metrics={metrics_mode_try} options=True: {err}",
                                flush=True,
                            )
                            continue
                        print(
                            f"[/sggg/portfolio] query failed metrics={metrics_mode_try} options=False: {err}",
                            flush=True,
                        )
                if rows:
                    break

            if not rows:
                raise RuntimeError("PSC portfolio query returned no rows for all SQL variants")
            fund_nav = None
            # PORTFOLIO_NAV is the last selected column
            if rows and rows[0] and rows[0][-1] is not None:
                try:
                    fund_nav = float(rows[0][-1])
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
                strike = _num(row[11]) if has_option_columns and len(row) > 11 else None
                security_delta = _num(row[12]) if has_option_columns and len(row) > 12 else None
                i = 13 if has_option_columns else 11

                exposure_pct_nav_frac = None
                fx_exposure_pct_nav = None
                beta_pct_nav = None

                if metrics_mode == "pct_full":
                    exposure_pct_nav_frac = _psc_pct_nav_fraction(row[i] if len(row) > i else None)
                    fx_exposure_pct_nav = _psc_pct_nav_fraction(row[i + 1] if len(row) > i + 1 else None)
                    beta_pct_nav = _psc_pct_nav_fraction(row[i + 2] if len(row) > i + 2 else None)
                    i += 3
                elif metrics_mode == "pct_fx_beta":
                    fx_exposure_pct_nav = _psc_pct_nav_fraction(row[i] if len(row) > i else None)
                    beta_pct_nav = _psc_pct_nav_fraction(row[i + 1] if len(row) > i + 1 else None)
                    i += 2
                elif metrics_mode == "loc_beta":
                    fx_loc = _num(row[i] if len(row) > i else None)
                    beta_pct_nav = _psc_pct_nav_fraction(row[i + 1] if len(row) > i + 1 else None)
                    if fund_nav and fund_nav != 0 and fx_loc is not None:
                        fx_exposure_pct_nav = fx_loc / fund_nav
                    i += 2

                qty_i, avg_i, close_i, pprof_i, fx_i, int_i, div_i, val_i, exp_i, dprof_i, nav_i = (
                    i,
                    i + 1,
                    i + 2,
                    i + 3,
                    i + 4,
                    i + 5,
                    i + 6,
                    i + 7,
                    i + 8,
                    i + 9,
                    i + 10,
                )
                exposure = _num(row[exp_i]) if len(row) > exp_i else None
                pct_nav = (
                    (exposure_pct_nav_frac * 100)
                    if exposure_pct_nav_frac is not None
                    else ((exposure / fund_nav * 100) if (fund_nav and fund_nav != 0 and exposure is not None) else None)
                )
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
                    "sedol": _str(row[10]),
                    "strike": strike,
                    "security_delta": security_delta,
                    "quantity": _num(row[qty_i]) if len(row) > qty_i else None,
                    "avg_price": _num(row[avg_i]) if len(row) > avg_i else None,
                    "close_price": _num(row[close_i]) if len(row) > close_i else None,
                    "price_profit": _num(row[pprof_i]) if len(row) > pprof_i else None,
                    "FX_SETTLE_TO_BASE": row[fx_i] if len(row) > fx_i else None,
                    "interest": _num(row[int_i]) if len(row) > int_i else None,
                    "dividends": _num(row[div_i]) if len(row) > div_i else None,
                    "value": _num(row[val_i]) if len(row) > val_i else None,
                    "exposure": exposure,
                    "exposure_pct_nav": pct_nav,
                    "fx_exposure_pct_nav": fx_exposure_pct_nav,
                    "beta_pct_nav": beta_pct_nav,
                    "day_profit": _num(row[dprof_i]) if len(row) > dprof_i else None,
                    "profit": None,  # PSC query doesn't have Profit column; can add if available
                })
        finally:
            if conn:
                conn.close()

        return jsonify({
            "fund": fund,
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
            "LONG_SHORT, QUANTITY, AVG_PRICE, CLOSE_PRICE, PRICE_PROFIT, FX_SETTLE_TO_BASE, POSN_OPEN_DT, POSN_CLOSE_DT, "
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
        filtered_out: List[dict] = []
        filtered_out_count = 0

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

                        security_name = _get("SecurityName") or ""
                        occ = _get("OCCSymbol") or ""
                        yellow_key = _get("YellowKey") or ""
                        asset_class = _get("AssetClass") or ""
                        sec_type = _get("Type") or ""  # often maps to the EMSX "Sec Type" column
                        # Keep EMSX-provided side verbatim; for options it may already be "Sell to Open"/etc.
                        side_raw = (_get("Side") or "").strip()
                        side = side_raw.upper()

                        def _looks_like_option_name(s: str) -> bool:
                            # EMSX typically formats options like: "SPY 04/24/26 P705"
                            return bool(re.search(r"\b\d{2}/\d{2}/\d{2}\s+[PC]\d", s or ""))

                        def _format_occ(occ_sym: str) -> Optional[str]:
                            # OCC: "SPY 260424P00705000" (or "SPY260424P00705000") -> "SPY 04/24/26 P705"
                            m = re.match(r"^([A-Z0-9]{1,6})\s*(\d{2})(\d{2})(\d{2})([CP])(\d{8})$", (occ_sym or "").strip().upper())
                            if not m:
                                return None
                            und, yy, mm, dd, pc, strike_raw = m.groups()
                            # OCC strike has 3 decimal places
                            strike = int(strike_raw) / 1000.0
                            strike_str = str(int(strike)) if abs(strike - int(strike)) < 1e-9 else f"{strike:.3f}".rstrip("0").rstrip(".")
                            return f"{und} {mm}/{dd}/{yy} {pc}{strike_str}"

                        # Determine if this fill is for an option.
                        # Be strict and structural: keep only rows that are definitively options, either:
                        # - OCC-style symbology (in OCCSymbol OR sometimes SecurityName), or
                        # - EMSX formatted option SecurityName (MM/DD/YY Pxxx / Cxxx).
                        # This avoids false positives where EMSX metadata labels equity/future rows oddly.
                        occ_candidate = (occ or "").strip() or security_name.strip()
                        is_option = bool(_format_occ(occ_candidate)) or _looks_like_option_name(security_name)
                        if not is_option:
                            filtered_out_count += 1
                            if len(filtered_out) < 25:
                                filtered_out.append({
                                    "SecurityName": security_name,
                                    "OCCSymbol": occ,
                                    "AssetClass": asset_class,
                                    "Type": sec_type,
                                    "YellowKey": yellow_key,
                                    "Side": side_raw,
                                    "DateTimeOfFill": _get("DateTimeOfFill"),
                                })
                            continue

                        # Sometimes OCCSymbol isn't populated, but SecurityName is an OCC-style string. Try both.
                        display = security_name.strip() if _looks_like_option_name(security_name) else (_format_occ(occ_candidate) or security_name.strip() or occ.strip())
                        # Use a stable key for grouping: canonicalize so it matches PSC symbols too.
                        security_key = (
                            _canonical_option_key(occ.strip())
                            or _canonical_option_key(occ_candidate)
                            or occ.strip()
                            or (occ_candidate.strip() if _format_occ(occ_candidate) else display)
                        )

                        fills.append({
                            "SECURITY": security_key,
                            "SECURITY_DISPLAY": display,
                            "OCC_SYMBOL": occ.strip() or None,
                            "TRADE_DATE_TIME": _get("DateTimeOfFill"),
                            "ORDER_ACTION": side_raw or side,
                            "ACT_QTTY": _get_num("FillShares"),
                            "PRICE": _get_num("FillPrice"),
                            "BROKER": _get("Broker"),
                            "ORDER_ID": _get("OrderId"),
                            "ROUTE_ID": _get("RouteId"),
                            "TICKER": _get("Ticker"),
                            "YELLOW_KEY": _get("YellowKey"),
                            "ASSET_CLASS": _get("AssetClass"),
                            "SEC_TYPE": sec_type or None,
                        })

            if et == blpapi.Event.RESPONSE:
                break

        # Attach debug info for troubleshooting filter behavior (only a few samples).
        for f in fills:
            if "FILTER_DEBUG" not in f:
                f["FILTER_DEBUG"] = {
                    "filtered_out_count": filtered_out_count,
                    "filtered_out_examples": filtered_out,
                }
                break
        return fills
    finally:
        try:
            session.stop()
        except Exception:
            pass


def _canonical_option_key(sym: str) -> Optional[str]:
    """
    Canonical key used to match the same option across data sources.

    Returns OCC-style canonical key without spaces:
      - "SPY 260424P00705000" -> "SPY260424P00705000"
      - "SPY 04/24/26 P705"   -> "SPY260424P00705000"
    """
    s = (sym or "").strip().upper()
    if not s:
        return None
    # PSC sometimes appends market/asset suffixes. Strip common noise.
    s = re.sub(r"\bEQUITY\b", "", s)
    s = re.sub(r"\bUS\b", "", s)
    s = s.replace(".US", "")
    s = re.sub(r"\s+", " ", s).strip()

    # Raw OCC: UNDERLYING + YYMMDD + P/C + STRIKE(8, 3dp implied)
    m = re.match(r"^([A-Z0-9]{1,6})\s*(\d{2})(\d{2})(\d{2})([CP])(\d{8})$", s)
    if m:
        und, yy, mm, dd, pc, strike_raw = m.groups()
        return f"{und}{yy}{mm}{dd}{pc}{strike_raw}"

    # EMSX display: "SPY 04/24/26 P705" (strike may have decimals)
    m2 = re.match(r"^([A-Z0-9]{1,6})\s+(\d{2})/(\d{2})/(\d{2})\s+([CP])\s*([0-9]+(?:\.[0-9]+)?)$", s)
    if m2:
        und, mm, dd, yy, pc, strike_str = m2.groups()
        try:
            strike = float(strike_str)
        except Exception:
            return None
        strike_raw = f"{int(round(strike * 1000.0)):08d}"
        return f"{und}{yy}{mm}{dd}{pc}{strike_raw}"

    # Alternate display: "SPY 20260424 P705" or "SPY 2026-04-24 P705"
    m3 = re.match(r"^([A-Z0-9]{1,6})\s+(\d{4})-?(\d{2})-?(\d{2})\s+([CP])\s*([0-9]+(?:\.[0-9]+)?)$", s)
    if m3:
        und, yyyy, mm, dd, pc, strike_str = m3.groups()
        yy = yyyy[2:4]
        try:
            strike = float(strike_str)
        except Exception:
            return None
        strike_raw = f"{int(round(strike * 1000.0)):08d}"
        return f"{und}{yy}{mm}{dd}{pc}{strike_raw}"

    return None


def _normalize_option_desc_key(sym: str) -> Optional[str]:
    """
    Normalize PSC/EMSX option description strings for dictionary key matching.
    Example: "XLE 04/24/26 C57 US Equity" -> "XLE 04/24/26 C57"
    """
    s = (sym or "").strip().upper()
    if not s:
        return None
    s = re.sub(r"\bEQUITY\b", "", s)
    s = re.sub(r"\bUS\b", "", s)
    s = s.replace(".US", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _options_closeout_analyze(trades: List[dict], starting_net_by_security: Optional[Dict[str, float]] = None) -> dict:
    """
    Analyze option fills for closeout mismatches / suspicious close-side usage.

    This is intentionally broker-agnostic and relies on PSC/EMSX-style order actions:
      - BUY, SELL, SELL SHORT, BUY COVR

    Returns:
      {
        trades: [ {security, trade_time, order_action, qty, price, broker, order_id, route_id, group_id, net_before, net_after, flag_reason?} ],
        groups: [ {group_id, security, start_time, end_time, net_end, is_open, flags_count, trades_count} ],
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

    def _event_key(e):
        return (_s(e.get("TRADE_DATE_INT")), _s(e.get("TRADE_DATE_TIME")), _s(e.get("ORDER_ID")))

    def _norm_action(order_action: str) -> str:
        oa = (order_action or "").strip()
        u = oa.upper()
        # EMSX Side mappings (exact, no inference):
        #   B  = Buy to Open
        #   BS = Buy to Close
        #   SS = Sell to Open
        #   S  = Sell to Close
        if u == "B":
            return "Buy to Open"
        if u == "BS":
            return "Buy to Close"
        if u == "SS":
            return "Sell to Open"
        if u == "S":
            return "Sell to Close"

        # Normalize common variants (PSC / other sources)
        if u == "BUY":
            return "Buy to Open"
        if u == "SELL":
            return "Sell to Close"
        if u == "SELL SHORT":
            return "Sell to Open"
        if u in ("BUY COVR", "BC"):
            return "Buy to Close"
        # EMSX-like strings
        if "BUY" in u and "OPEN" in u:
            return "Buy to Open"
        if "BUY" in u and "CLOSE" in u:
            return "Buy to Close"
        if "SELL" in u and "OPEN" in u:
            return "Sell to Open"
        if "SELL" in u and "CLOSE" in u:
            return "Sell to Close"
        return oa

    def _signed_delta(order_action: str, qty_abs: float) -> float:
        oa = _norm_action(order_action)
        u = oa.upper()
        if u == "BUY TO OPEN":
            return +qty_abs
        if u == "SELL TO CLOSE":
            return -qty_abs
        if u == "SELL TO OPEN":
            return -qty_abs
        if u == "BUY TO CLOSE":
            return +qty_abs
        return 0.0

    # Consolidate partial fills into order-level rows (replicates EMSX UI behavior more closely).
    # Key = (SECURITY, ORDER_ID, ORDER_ACTION).
    order_map: Dict[str, dict] = {}
    for t in trades:
        sec_raw = _s(t.get("SECURITY"))
        sec = _canonical_option_key(sec_raw) or sec_raw
        if not sec:
            continue
        oa = _s(t.get("ORDER_ACTION") or t.get("ORDER") or "")
        order_id = _s(t.get("ORDER_ID"))
        if not order_id:
            # If no order id, treat the fill as its own order.
            order_id = f"NO_ORDER::{sec}::{_s(t.get('TRADE_DATE_TIME'))}"
        key = f"{sec}::{order_id}::{oa.upper()}"

        qty = _n(t.get("ACT_QTTY")) or _n(t.get("FILLED_QTTY")) or 0.0
        qty_abs = abs(qty)
        price = _n(t.get("PRICE"))
        tt = _s(t.get("TRADE_DATE_TIME"))
        td = _s(t.get("TRADE_DATE_INT"))
        broker = _s(t.get("BROKER"))
        route_id = _s(t.get("ROUTE_ID"))

        if key not in order_map:
            order_map[key] = {
                "SECURITY": sec,
                "SECURITY_DISPLAY": _s(t.get("SECURITY_DISPLAY")) or sec,
                "ORDER_ID": order_id,
                "ORDER_ACTION": oa,
                "TRADE_DATE_INT": td,
                "TRADE_DATE_TIME": tt,
                "TRADE_TIME_FIRST": tt,
                "TRADE_TIME_LAST": tt,
                "BROKER": broker,
                "ROUTE_ID": route_id,
                "QTY_ABS": 0.0,
                "NOTIONAL": 0.0,  # qty_abs * price
            }

        acc = order_map[key]
        acc["TRADE_DATE_INT"] = acc["TRADE_DATE_INT"] or td
        # Keep first/last time
        if tt:
            if not acc.get("TRADE_TIME_FIRST") or tt < acc["TRADE_TIME_FIRST"]:
                acc["TRADE_TIME_FIRST"] = tt
            if not acc.get("TRADE_TIME_LAST") or tt > acc["TRADE_TIME_LAST"]:
                acc["TRADE_TIME_LAST"] = tt
        acc["QTY_ABS"] += qty_abs
        if price is not None:
            acc["NOTIONAL"] += qty_abs * float(price)
        # Prefer a non-empty broker/route_id if present
        if broker and not acc.get("BROKER"):
            acc["BROKER"] = broker
        if route_id and not acc.get("ROUTE_ID"):
            acc["ROUTE_ID"] = route_id

    consolidated_orders: List[dict] = []
    for _, acc in order_map.items():
        qty_abs = float(acc.get("QTY_ABS") or 0.0)
        notional = float(acc.get("NOTIONAL") or 0.0)
        vwap = (notional / qty_abs) if (qty_abs and notional) else None
        consolidated_orders.append({
            "SECURITY": acc.get("SECURITY"),
            "SECURITY_DISPLAY": acc.get("SECURITY_DISPLAY") or acc.get("SECURITY"),
            "ORDER_ID": acc.get("ORDER_ID"),
            "ORDER_ACTION": acc.get("ORDER_ACTION"),
            "TRADE_DATE_INT": acc.get("TRADE_DATE_INT"),
            # Use the first fill time as the order time to match EMSX list view; keep last separately for debugging if needed.
            "TRADE_DATE_TIME": acc.get("TRADE_TIME_FIRST") or acc.get("TRADE_DATE_TIME"),
            "TRADE_TIME_FIRST": acc.get("TRADE_TIME_FIRST"),
            "TRADE_TIME_LAST": acc.get("TRADE_TIME_LAST"),
            "ACT_QTTY": qty_abs,
            "PRICE": vwap,
            "BROKER": acc.get("BROKER"),
            "ROUTE_ID": acc.get("ROUTE_ID"),
        })

    grouped: Dict[str, List[dict]] = {}
    for t in consolidated_orders:
        sec = _s(t.get("SECURITY"))
        if not sec:
            continue
        grouped.setdefault(sec, []).append(t)

    normalized_trades: List[dict] = []
    groups_out: List[dict] = []
    open_positions = []
    suspicious_actions = []
    by_security = {}

    for sec, ts in grouped.items():
        ts_sorted = sorted(ts, key=_trade_key)
        net = float((starting_net_by_security or {}).get(sec, 0.0) or 0.0)
        first_time = None
        last_time = None

        # Grouping for this report: one group per option security.
        current_gid = sec
        group_start_time = None
        group_flags = 0
        group_trades = 0

        for t in ts_sorted:
            oa_raw = _s(t.get("ORDER_ACTION") or t.get("ORDER") or "")
            oa = _norm_action(oa_raw)
            qty = _n(t.get("ACT_QTTY")) or _n(t.get("FILLED_QTTY")) or 0.0
            qty_abs = abs(qty)
            if qty_abs == 0:
                continue

            trade_time = _s(t.get("TRADE_DATE_TIME"))
            order_id = _s(t.get("ORDER_ID"))
            route_id = _s(t.get("ROUTE_ID"))
            broker = _s(t.get("BROKER"))
            price = _n(t.get("PRICE"))

            net_before = net
            flag_reason = None

            if group_start_time is None:
                group_start_time = trade_time or None

            # Flag suspicious "open vs close" usage based on existing position.
            # What you want to catch: using an OPEN instruction when you intended to CLOSE.
            if net_before > 0 and oa.upper() == "SELL TO OPEN":
                flag_reason = "Position is long; this should typically be Sell to Close (not Sell to Open)"
                suspicious_actions.append({
                    "security": sec,
                    "trade_time": trade_time,
                    "order_action": oa,
                    "qty": qty,
                    "net_before": net_before,
                    "reason": flag_reason,
                })
            if net_before < 0 and oa.upper() == "BUY TO OPEN":
                flag_reason = "Position is short; this should typically be Buy to Close (not Buy to Open)"
                suspicious_actions.append({
                    "security": sec,
                    "trade_time": trade_time,
                    "order_action": oa,
                    "qty": qty,
                    "net_before": net_before,
                    "reason": flag_reason,
                })

            net += _signed_delta(oa, qty_abs)
            first_time = first_time or trade_time
            last_time = trade_time or last_time

            group_trades += 1
            if flag_reason:
                group_flags += 1

            normalized_trades.append({
                "security": sec,
                "security_display": _s(t.get("SECURITY_DISPLAY")) or sec,
                "trade_time": trade_time or None,
                "order_action": oa or None,
                "qty": qty,
                "price": price,
                "broker": broker or None,
                "order_id": order_id or None,
                "route_id": route_id or None,
                "group_id": current_gid,
                "net_before": net_before,
                "net_after": net,
                "flag_reason": flag_reason,
            })

            # No flat-to-flat segmentation; keep accumulating.

        by_security[sec] = {
            "net_contracts": net,
            "first_time": first_time,
            "last_time": last_time,
            "trades_count": len(ts_sorted),
            "starting_net": float((starting_net_by_security or {}).get(sec, 0.0) or 0.0),
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

        # Emit one group summary per security.
        groups_out.append({
            "group_id": current_gid,
            "security": sec,
            "security_display": _s(ts_sorted[0].get("SECURITY_DISPLAY")) or sec if ts_sorted else sec,
            "executed_time": last_time,
            "net_end": net,
            "is_open": abs(net) > 1e-9,
            "flags_count": group_flags,
            "trades_count": group_trades,
        })

    return {
        "trades": normalized_trades,
        "groups": groups_out,
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

        # Optional: start-of-day positions from PSC (yesterday/last snapshot) to correctly interpret intraday exits.
        starting_net_by_security: Dict[str, float] = {}
        starting_positions_date = None
        if bool(data.get("use_psc_start_positions") is True):
            try:
                # Resolve PSC portfolios. For "All funds" (no specific mapping), aggregate all known portfolios.
                portfolio = _psc_portfolio_from_request(data)
                portfolios: List[str] = [portfolio] if portfolio else list(PSC_ALL_FUNDS_PORTFOLIOS)

                # Use the latest snapshot strictly prior to the report date.
                # This naturally handles weekends/holidays without needing a trading-day calendar.
                report_compact = _ymd_to_compact(date_iso)
                cache_key = f"{report_compact}||{','.join(portfolios)}"
                if cache_key in _PSC_START_POS_CACHE:
                    starting_net_by_security = dict(_PSC_START_POS_CACHE.get(cache_key) or {})
                    starting_positions_date = (_PSC_START_POS_CACHE_META.get(cache_key) or {}).get("starting_positions_date")
                else:
                    pyodbc, err = _pyodbc_or_503()
                    if not err and pyodbc is not None:
                        conn = pyodbc.connect("DSN=PSC_VIEWER")
                        cursor = conn.cursor()

                        def _accumulate_rows(rows: list):
                            for rr in rows or []:
                                desc_raw = (str(rr[0]).strip() if rr and rr[0] is not None else "")
                                canon = _canonical_option_key(desc_raw)
                                desc_key = _normalize_option_desc_key(desc_raw) or desc_raw.upper()
                                sec = canon or desc_key
                                ls = (str(rr[1]).strip().upper() if len(rr) > 1 and rr[1] is not None else "")
                                qty = float(rr[2]) if len(rr) > 2 and rr[2] is not None else 0.0
                                if not sec:
                                    continue
                                # PSC QUANTITY is already signed (shorts are typically negative).
                                # Using LONG_SHORT to re-sign can double-flip and break aggregation across funds.
                                signed = qty
                                starting_net_by_security[sec] = starting_net_by_security.get(sec, 0.0) + signed
                                if canon and canon != sec:
                                    starting_net_by_security[canon] = starting_net_by_security.get(canon, 0.0) + signed
                                if desc_key and desc_key != sec:
                                    starting_net_by_security[desc_key] = starting_net_by_security.get(desc_key, 0.0) + signed

                        placeholders = ",".join(["?"] * len(portfolios))
                        sec_type_filter = "SECURITY_TYPE IN ('EquityOption','Equity Option')"

                        if len(portfolios) == 1:
                            # Single-fund fast path: one probe + one fetch.
                            snap = _psc_probe_snapshot_before(cursor, portfolios[0], report_compact, lookback_days=14) or ""
                            starting_positions_date = snap or None
                            if snap:
                                cursor.execute(
                                    "SELECT DESCRIPTION, LONG_SHORT, QUANTITY, SECURITY_TYPE, PORTFOLIO FROM psc_position_history "
                                    "WHERE PORTFOLIO = ? AND POSN_DATE = ? AND " + sec_type_filter,
                                    (portfolios[0], snap),
                                )
                                _accumulate_rows(cursor.fetchall())
                        else:
                            # All-funds path: portfolios can have different latest snapshots.
                            # Resolve a snapshot per portfolio (fast probes), then fetch per snapshot bucket.
                            snap_by_portfolio: Dict[str, str] = {}
                            for p in portfolios:
                                snap = _psc_probe_snapshot_before(cursor, p, report_compact, lookback_days=14)
                                if snap:
                                    snap_by_portfolio[p] = snap

                            snaps_used = sorted(set(snap_by_portfolio.values()))
                            starting_positions_date = (max(snaps_used) if snaps_used else None)

                            for snap in snaps_used:
                                ps = [p for p, s in snap_by_portfolio.items() if s == snap]
                                if not ps:
                                    continue
                                ps_placeholders = ",".join(["?"] * len(ps))
                                cursor.execute(
                                    "SELECT DESCRIPTION, LONG_SHORT, QUANTITY, SECURITY_TYPE, PORTFOLIO FROM psc_position_history "
                                    f"WHERE PORTFOLIO IN ({ps_placeholders}) AND POSN_DATE = ? AND {sec_type_filter}",
                                    tuple(ps) + (snap,),
                                )
                                _accumulate_rows(cursor.fetchall())

                        try:
                            conn.close()
                        except Exception:
                            pass

                        _PSC_START_POS_CACHE[cache_key] = dict(starting_net_by_security)
                        _PSC_START_POS_CACHE_META[cache_key] = {"starting_positions_date": starting_positions_date}
            except Exception:
                # Don't fail the EMSX check if PSC isn't reachable; just proceed with zero start positions.
                starting_net_by_security = starting_net_by_security or {}

        try:
            trades = _emsx_history_get_fills(date_iso=date_iso, uuids=uuids, team=team)
            analysis = _options_closeout_analyze(_log_serialize(trades), starting_net_by_security=starting_net_by_security)
            return _json_response({
                "source": "emsx_history",
                "build": DATA_BRIDGE_BUILD,
                "date": date_iso,
                "trade_count": len(trades),
                "scope": {"team": team, "uuids": uuids},
                "starting_positions_date": starting_positions_date,
                "starting_positions_count": len(starting_net_by_security),
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


@app.route("/sggg/diamond/nav-sheet", methods=["GET", "POST"])
def sggg_diamond_nav_sheet():
    """
    Get NAV sheet (per-class NAVPU and daily return) from SGGG Diamond API.
    Body or query: fund_id, valuation_date (yyyy-mm-dd)
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = request.args.get("fund_id") or data.get("fund_id")
        if not fund_id:
            fund_id = _get_default_fund_id()
        raw_date = (
            request.args.get("date")
            or request.args.get("valuation_date")
            or data.get("date")
            or data.get("valuation_date")
        )
        if not raw_date:
            raw_date = datetime.now().strftime("%Y-%m-%d")
        valuation_date = normalize_valuation_date(str(raw_date))
        raw = client.get_nav_sheet(fund_id=fund_id, valuation_date=valuation_date)
        summary = parse_nav_sheet_summary(raw)
        return jsonify(
            {
                "status": "available" if summary.get("available") else "unavailable",
                "valuation_date": valuation_date,
                "fund_id": fund_id,
                "raw": raw,
                "summary": summary,
            }
        )
    except DiamondNavUnavailableError as exc:
        return jsonify(
            {
                "status": "unavailable",
                "valuation_date": valuation_date,
                "fund_id": fund_id,
                "unavailable_end_date": exc.end_date,
                "message": exc.user_message,
            }
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def _boxed_positions_for_fund_id(
    boxed_by_fund: Dict[str, List[Dict[str, Any]]],
    fund_id: str,
) -> List[Dict[str, Any]]:
    fid = (fund_id or "").strip()
    if not fid:
        return []
    if fid in boxed_by_fund:
        return list(boxed_by_fund[fid] or [])
    fid_up = fid.upper()
    for key, rows in boxed_by_fund.items():
        if (key or "").strip().upper() == fid_up:
            return list(rows or [])
    return []


@app.route("/sggg/psc/boxed-positions", methods=["GET", "POST"])
def sggg_psc_boxed_positions():
    """
    AlphaDesk PSC boxed positions for NAV checker funds (same logic as scripts/run_psc_boxed_live.py).
    Body: valuation_date, optional funds: [{id, name}, ...]
    """
    try:
        data = request.get_json(silent=True) or {}
        raw_date = (
            request.args.get("date")
            or request.args.get("valuation_date")
            or data.get("date")
            or data.get("valuation_date")
        )
        if not raw_date:
            return jsonify({"error": "valuation_date required"}), 400
        valuation_date = normalize_valuation_date(str(raw_date))

        funds_in = data.get("funds")
        if isinstance(funds_in, list) and funds_in:
            fund_specs = [
                {"id": str(f.get("id") or f.get("fund_id") or "").strip(), "name": str(f.get("name") or "").strip()}
                for f in funds_in
                if isinstance(f, dict) and (f.get("id") or f.get("fund_id"))
            ]
        else:
            fund_specs = [{"id": fid, "name": ""} for fid in _get_diamond_fund_ids()]

        dsn = (os.environ.get("SGGG_PSC_ODBC_DSN") or "PSC_VIEWER").strip() or "PSC_VIEWER"
        t0 = time.time()
        boxed_by_fund, positions_by_fund, err, posn_dates_by_fund = fetch_boxed_positions_for_funds(
            fund_specs,
            valuation_date,
            store_portfolios=True,
            dsn=dsn,
        )
        elapsed = time.time() - t0
        total_boxes = sum(len(v or []) for v in boxed_by_fund.values())
        return jsonify(
            {
                "valuation_date": valuation_date,
                "by_fund": boxed_by_fund,
                "psc_position_counts": {
                    spec["id"]: len(positions_by_fund.get(spec["id"]) or [])
                    for spec in fund_specs
                },
                "psc_box_counts": {
                    spec["id"]: len(boxed_by_fund.get(spec["id"]) or [])
                    for spec in fund_specs
                },
                "psc_posn_dates_by_fund": posn_dates_by_fund,
                "total_boxes": total_boxes,
                "error": err,
                "psc_boxed_sec": round(elapsed, 2),
                "nav_checker_build": "sggg-psc-boxed-v17",
            }
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/sggg/close-price-reconciliation", methods=["GET", "POST"])
def sggg_close_price_reconciliation():
    """
    Line-by-line closing price comparison: Diamond GetPortfolio vs AlphaDesk PSC.
    Body: fund_id, valuation_date (yyyy-mm-dd).
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured."}), 503
        data = request.get_json(silent=True) or {}
        fund_id = (request.args.get("fund_id") or data.get("fund_id") or "").strip()
        raw_date = (
            request.args.get("date")
            or request.args.get("valuation_date")
            or data.get("date")
            or data.get("valuation_date")
        )
        if not fund_id:
            return jsonify({"error": "fund_id required"}), 400
        if not raw_date:
            return jsonify({"error": "valuation_date required"}), 400
        valuation_date = normalize_valuation_date(str(raw_date))
        t0 = time.time()
        lines, meta, err = fetch_close_price_reconciliation(fund_id, valuation_date, client)
        if err:
            return jsonify({"error": err, "meta": meta}), 502
        return jsonify(
            {
                "fund_id": fund_id,
                "valuation_date": valuation_date,
                "lines": lines,
                "meta": meta,
                "timing_sec": round(time.time() - t0, 2),
                "nav_checker_build": "sggg-close-price-v27",
            }
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/sggg/diamond/nav-availability", methods=["GET", "POST"])
def sggg_diamond_nav_availability():
    """
    NAV availability summary for one or more funds on a valuation date.
    Body: valuation_date, optional funds: [{id, name}, ...] — defaults to SGGG_DIAMOND_FUND_IDS with unknown names.
    """
    try:
        client = get_diamond_client()
        if not client:
            return jsonify({"error": "Diamond API not configured. Set SGGG_DIAMOND_USERNAME, SGGG_DIAMOND_PASSWORD."}), 503
        data = request.get_json(silent=True) or {}
        raw_date = (
            request.args.get("date")
            or request.args.get("valuation_date")
            or data.get("date")
            or data.get("valuation_date")
        )
        if not raw_date:
            raw_date = datetime.now().strftime("%Y-%m-%d")
        valuation_date = normalize_valuation_date(str(raw_date))

        funds_in = data.get("funds")
        if isinstance(funds_in, list) and funds_in:
            fund_specs = [
                {"id": str(f.get("id") or f.get("fund_id") or "").strip(), "name": str(f.get("name") or "").strip()}
                for f in funds_in
                if isinstance(f, dict) and (f.get("id") or f.get("fund_id"))
            ]
        else:
            fund_specs = [{"id": fid, "name": ""} for fid in _get_diamond_fund_ids()]

        auth_key = client._ensure_auth()
        started = time.time()
        results: List[Dict[str, Any]] = [None] * len(fund_specs)  # type: ignore[list-item]

        prior_date = prior_business_day_iso(valuation_date)
        fund_ids_list = [spec["id"] for spec in fund_specs]

        include_prior_diamond = os.environ.get("SGGG_NAV_CHECKER_PRIOR_DIAMOND", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        use_psc = os.environ.get("SGGG_NAV_CHECKER_USE_PSC", "0").strip().lower() in ("1", "true", "yes")
        force_diamond = data.get("force_diamond") in (True, 1) or str(
            data.get("force_diamond", "")
        ).strip().lower() in ("1", "true", "yes")

        diamond_short_circuit: Optional[DiamondNavUnavailableError] = None
        diamond_skip_reason: Optional[str] = None
        if not force_diamond:
            early_msg = should_skip_diamond_early_for_today(valuation_date)
            if early_msg:
                diamond_short_circuit = DiamondNavUnavailableError(
                    early_msg,
                    end_date=valuation_date,
                )
                diamond_skip_reason = "early_today"

        db_snapshots: Dict[tuple, Dict[str, Any]] = {}
        if not force_diamond and supabase:
            prior_lookup_dates = list(
                dict.fromkeys(
                    [valuation_date, prior_date]
                    + prior_business_days_for_lookup(prior_date)
                )
            )
            db_snapshots = load_snapshots_bulk(
                supabase,
                fund_ids_list,
                prior_lookup_dates,
            )

        def _base_entry(spec: Dict[str, str], wp: Dict[str, Any], wp_prior: Dict[str, Any], psc_navs: Dict) -> Dict[str, Any]:
            fid = spec["id"]
            name = spec.get("name") or fid
            est_row = (wp.get("estimates_by_fund_id") or {}).get(fid) or {}
            est_prior = (wp_prior.get("estimates_by_fund_id") or {}).get(fid) or {}
            native_ccy = FUND_NATIVE_CURRENCY.get(fid, "CAD")
            entry: Dict[str, Any] = {
                "fund_id": fid,
                "fund_name": name,
                "status": "unavailable",
                "error": None,
                "message": None,
                "classes": [],
                "prior_valuation_date": prior_date,
                "diamond_prior_valuation_date": None,
                "opening_nav_aum": None,
                "closing_nav_aum": None,
                "sggg_nav_change_dollars": None,
                "class_i_bps": None,
                "estimate_nav_change_dollars": None,
                "estimate_bps": None,
                "bps_difference": None,
                "nav_change_difference_dollars": None,
                "opening_aum_source": None,
                "closing_aum_source": None,
                "compliance_opening_aum": None,
                "compliance_closing_aum": None,
                "ehp_nav_change_dollars": None,
                "ehp_aum_change_unadjusted": None,
                "compliance_net_subs_reds": None,
                "diamond_opening_aum": None,
                "diamond_closing_aum": None,
                "diamond_aum_currency": native_ccy,
                "aum_currency": native_ccy,
                "sggg_nav_change_raw": None,
                "capital_flow_adjustment": None,
                "capital_flow_adjustment_source": None,
                "capital_flow_adjustment_label": None,
                "diamond_open_aum_components": None,
                "diamond_close_aum_components": None,
                "diamond_capital_flow_candidates": None,
                "diamond_prior_eod_aum": None,
                "diamond_prior_day_flow_adjustment": None,
                "aum_parse_version": None,
                "diamond_opening_aum_source": None,
                "sggg_nav_change_note": None,
                "_estimate_bps_from_compliance": est_row.get("estimate_bps") is not None,
                "_est_row": est_row,
                "_est_prior": est_prior,
            }
            if entry["_estimate_bps_from_compliance"]:
                entry["estimate_bps"] = int(est_row["estimate_bps"])
            if est_row.get("spreadsheet_label"):
                entry["spreadsheet_label"] = est_row.get("spreadsheet_label")
            if est_row.get("ror_display"):
                entry["estimate_ror_display"] = est_row.get("ror_display")
            if est_row.get("prior_eod_aum") is not None:
                entry["opening_nav_aum"] = float(est_row["prior_eod_aum"])
                entry["opening_aum_source"] = "compliance"
                entry["aum_currency"] = est_row.get("aum_currency") or native_ccy
            if est_row.get("current_aum") is not None:
                entry["closing_nav_aum"] = float(est_row["current_aum"])
                entry["closing_aum_source"] = "compliance"
                entry["aum_currency"] = est_row.get("aum_currency") or native_ccy
            if est_row.get("prior_eod_aum") is not None and est_row.get("current_aum") is not None:
                entry["compliance_opening_aum"] = float(est_row["prior_eod_aum"])
                entry["compliance_closing_aum"] = float(est_row["current_aum"])
                entry["ehp_aum_change_unadjusted"] = (
                    entry["compliance_closing_aum"] - entry["compliance_opening_aum"]
                )
                if est_row.get("net_subs_reds") is not None:
                    entry["compliance_net_subs_reds"] = float(est_row["net_subs_reds"])
                entry["ehp_nav_change_dollars"] = compliance_aum_change_ex_flows(
                    entry["compliance_opening_aum"],
                    entry["compliance_closing_aum"],
                    est_row.get("net_subs_reds"),
                )
            psc_row = psc_navs.get(fid) or {}
            if entry["opening_nav_aum"] is None and psc_row.get("opening") is not None:
                entry["opening_nav_aum"] = float(psc_row["opening"])
                entry["opening_aum_source"] = "psc"
                entry["aum_currency"] = "CAD"
            if entry["closing_nav_aum"] is None and psc_row.get("closing") is not None:
                entry["closing_nav_aum"] = float(psc_row["closing"])
                entry["closing_aum_source"] = "psc"
                entry["aum_currency"] = "CAD"
            return entry

        # Diamond close sheets first; prior-day sheets only if close is available (saves calls when NAV not out).
        diamond_close_tasks: List[tuple] = []
        if not diamond_short_circuit:
            for idx, spec in enumerate(fund_specs):
                diamond_close_tasks.append((idx, spec["id"], valuation_date, "close"))

        diamond_workers = min(
            int(os.environ.get("SGGG_DIAMOND_NAV_MAX_WORKERS", "12")),
            max(1, len(diamond_close_tasks) or 1),
        )
        prep_slots = 3 if use_psc else 2
        pool_workers = max(diamond_workers, prep_slots)

        wp: Dict[str, Any] = {}
        wp_prior: Dict[str, Any] = {}
        psc_navs: Dict[str, Dict[str, Optional[float]]] = {}
        diamond_results: Dict[tuple, Dict[str, Any]] = {}
        diamond_errors: Dict[tuple, Any] = {}
        diamond_call_secs: List[float] = []
        diamond_cache_hits = 0
        diamond_db_hits = 0
        diamond_calls_detail: List[Dict[str, Any]] = []
        compliance_sec = 0.0
        compliance_prior_sec = 0.0
        psc_sec = 0.0
        diamond_api_base = client.base_url.rstrip("/")

        def _diamond_call_purpose(role: str) -> str:
            if role == "close":
                return (
                    "Report-day GetNAVSheet: per-class NAVPU, daily return (bps), "
                    "Class I return, closing fund NetAssetValue"
                )
            return (
                "Prior business day GetNAVSheet: opening fund NetAssetValue for "
                "SGGG day-change (close AUM minus open AUM, net of subs)"
            )

        def _fetch_summary_for_date(
            fid: str,
            vdate: str,
            vdate_norm: str,
        ) -> Dict[str, Any]:
            """Load one GetNAVSheet summary from DB, memory cache, or live API."""
            if not force_diamond:
                db_summary = db_snapshots.get((fid, vdate_norm))
                if db_summary and snapshot_usable(db_summary):
                    return {
                        "summary": db_summary,
                        "error": None,
                        "data_source": "supabase",
                        "cache_hit": True,
                        "duration_sec": 0.0,
                        "http_status": 200,
                        "status": "from_database",
                    }
            if not force_diamond:
                cached_raw = get_nav_sheet_raw_cached(fid, vdate)
                if cached_raw is not None:
                    summary = parse_nav_sheet_summary(cached_raw)
                    if fund_aum_from_summary(summary) is not None:
                        return {
                            "summary": summary,
                            "error": None,
                            "data_source": "memory",
                            "cache_hit": True,
                            "duration_sec": 0.0,
                            "http_status": 200,
                            "status": "from_cache",
                        }
            t_call = time.time()
            try:
                raw = fetch_nav_sheet(diamond_api_base, fid, vdate, auth_key=auth_key)
                elapsed = time.time() - t_call
                summary = parse_nav_sheet_summary(raw)
                if nav_sheet_summary_cacheable(summary):
                    set_nav_sheet_raw_cached(fid, vdate, raw)
                    upsert_snapshot(supabase, fid, vdate, summary)
                return {
                    "summary": summary,
                    "error": None,
                    "data_source": "live",
                    "cache_hit": False,
                    "duration_sec": round(elapsed, 2),
                    "http_status": 200,
                    "status": "available" if summary.get("available") else "response_ok_no_class_nav",
                }
            except DiamondNavUnavailableError as exc:
                return {
                    "summary": None,
                    "error": exc,
                    "data_source": "live",
                    "cache_hit": False,
                    "duration_sec": round(time.time() - t_call, 2),
                    "http_status": 400,
                    "status": "not_finalized",
                }
            except Exception as exc:
                return {
                    "summary": None,
                    "error": exc,
                    "data_source": "live",
                    "cache_hit": False,
                    "duration_sec": round(time.time() - t_call, 2),
                    "http_status": None,
                    "status": "error",
                }

        def _fetch_nav_task(task: tuple) -> Dict[str, Any]:
            t0 = time.time()
            idx, fid, vdate, role = task
            fund_name = (fund_specs[idx].get("name") or "").strip() if idx < len(fund_specs) else fid
            log: Dict[str, Any] = {
                "fund_index": idx,
                "fund_id": fid,
                "fund_name": fund_name or fid,
                "role": role,
                "purpose": _diamond_call_purpose(role),
                "http_method": "POST",
                "endpoint_path": "/api/v1/GetNAVSheet/",
                "url": f"{diamond_api_base}/GetNAVSheet/",
                "request_headers_note": "Authorization: AuthKey <token>; AuthKey: <token>; Content-Type: application/json",
                "force_diamond": force_diamond,
            }
            vdate_norm = normalize_valuation_date(vdate)
            val_norm = normalize_valuation_date(valuation_date)

            if role == "open":
                lookback = int(os.environ.get("SGGG_NAV_PRIOR_LOOKBACK_DAYS", "12"))
                candidates = prior_business_days_for_lookup(vdate_norm, max_days=lookback)
                fetch_attempts: List[Dict[str, Any]] = []
                last_error: Any = None
                chosen_summary: Optional[Dict[str, Any]] = None
                effective_prior: Optional[str] = None
                for candidate in candidates:
                    log["request_body"] = {"FundID": fid, "ValuationDate": candidate}
                    attempt = _fetch_summary_for_date(fid, candidate, candidate)
                    summary = attempt.get("summary")
                    sheet_date = None
                    if summary:
                        sheet_date = normalize_diamond_sheet_date(
                            summary.get("sheet_valuation_date")
                            or summary.get("valuation_date")
                        )
                    usable, reject_reason = prior_open_sheet_is_usable(
                        sheet_date, val_norm, candidate
                    )
                    has_aum = summary is not None and fund_aum_from_summary(summary) is not None
                    fetch_attempts.append(
                        {
                            "requested_date": candidate,
                            "sheet_valuation_date": sheet_date,
                            "usable": usable and has_aum,
                            "reject_reason": reject_reason,
                            "has_fund_aum": has_aum,
                            "data_source": attempt.get("data_source"),
                            "status": attempt.get("status"),
                        }
                    )
                    if attempt.get("error"):
                        last_error = attempt["error"]
                    if usable and has_aum and summary:
                        chosen_summary = summary
                        effective_prior = sheet_date or candidate
                        break
                elapsed = time.time() - t0
                if chosen_summary is not None and effective_prior:
                    chosen_summary = dict(chosen_summary)
                    chosen_summary["prior_effective_valuation_date"] = effective_prior
                    chosen_summary["prior_holiday_fallback"] = effective_prior != vdate_norm
                    chosen_summary["prior_fetch_attempts"] = fetch_attempts
                    log.update(
                        {
                            "request_body": {"FundID": fid, "ValuationDate": effective_prior},
                            "duration_sec": round(elapsed, 2),
                            "http_status": 200,
                            "cache_hit": False,
                            "status": "available",
                            "response_valuation_date": chosen_summary.get("valuation_date"),
                            "sheet_valuation_date": normalize_diamond_sheet_date(
                                chosen_summary.get("sheet_valuation_date")
                                or chosen_summary.get("valuation_date")
                            ),
                            "request_valuation_date": effective_prior,
                            "initial_prior_business_day": vdate_norm,
                            "prior_holiday_fallback": effective_prior != vdate_norm,
                            "prior_fetch_attempts": fetch_attempts,
                            "class_count": len(chosen_summary.get("classes") or []),
                            "fund_aum": fund_aum_from_summary(chosen_summary),
                        }
                    )
                    return {
                        "idx": idx,
                        "role": role,
                        "summary": chosen_summary,
                        "log": log,
                        "error": None,
                    }
                log.update(
                    {
                        "duration_sec": round(elapsed, 2),
                        "initial_prior_business_day": vdate_norm,
                        "prior_fetch_attempts": fetch_attempts,
                    }
                )
                if isinstance(last_error, DiamondNavUnavailableError):
                    log.update(
                        {
                            "http_status": 400,
                            "status": "not_finalized",
                            "error_message": last_error.user_message,
                            "diamond_end_date": last_error.end_date,
                        }
                    )
                    return {"idx": idx, "role": role, "summary": None, "log": log, "error": last_error}
                if isinstance(last_error, Exception):
                    log.update({"status": "error", "error_message": str(last_error)[:2000]})
                    return {"idx": idx, "role": role, "summary": None, "log": log, "error": last_error}
                log.update(
                    {
                        "status": "not_found",
                        "error_message": (
                            f"No usable prior GetNAVSheet before {val_norm} "
                            f"(tried {len(fetch_attempts)} business days back from {vdate_norm})"
                        ),
                    }
                )
                return {"idx": idx, "role": role, "summary": None, "log": log, "error": None}

            log["request_body"] = {"FundID": fid, "ValuationDate": vdate}
            attempt = _fetch_summary_for_date(fid, vdate, vdate_norm)
            summary = attempt.get("summary")
            log.update(
                {
                    "duration_sec": attempt.get("duration_sec"),
                    "http_status": attempt.get("http_status"),
                    "cache_hit": attempt.get("cache_hit"),
                    "data_source": attempt.get("data_source"),
                    "status": attempt.get("status"),
                    "request_valuation_date": vdate_norm,
                }
            )
            if summary is not None:
                log.update(
                    {
                        "response_valuation_date": summary.get("valuation_date"),
                        "sheet_valuation_date": summary.get("sheet_valuation_date"),
                        "class_count": len(summary.get("classes") or []),
                        "fund_aum": fund_aum_from_summary(summary),
                        "capital_flow": summary.get("capital_flow"),
                        "capital_flow_candidate_count": len(
                            summary.get("capital_flow_candidates") or []
                        ),
                        "aum_parse_version": summary.get("aum_parse_version"),
                    }
                )
                return {"idx": idx, "role": role, "summary": summary, "log": log, "error": None}
            err = attempt.get("error")
            if isinstance(err, DiamondNavUnavailableError):
                log.update(
                    {
                        "error_message": err.user_message,
                        "diamond_end_date": err.end_date,
                    }
                )
            elif isinstance(err, Exception):
                log.update({"error_message": str(err)[:2000]})
            return {"idx": idx, "role": role, "summary": None, "log": log, "error": err}

        def _timed_estimates(val_d):
            t0 = time.time()
            try:
                return estimates_by_fund_id(val_d), time.time() - t0
            except Exception as exc:
                return (
                    {"available": False, "error": str(exc), "estimates_by_fund_id": {}},
                    time.time() - t0,
                )

        def _timed_psc():
            t0 = time.time()
            try:
                return fetch_psc_portfolio_navs(fund_ids_list, prior_date, valuation_date), time.time() - t0
            except Exception:
                return {}, time.time() - t0

        from datetime import date as _date

        val_d = _date.fromisoformat(valuation_date)
        prior_d = _date.fromisoformat(prior_date)

        if diamond_short_circuit:
            for idx in range(len(fund_specs)):
                diamond_errors[(idx, "close")] = diamond_short_circuit

        def _apply_diamond_result(result: Dict[str, Any]) -> None:
            nonlocal diamond_cache_hits, diamond_db_hits
            key = (result["idx"], result["role"])
            diamond_calls_detail.append(result["log"])
            if result["log"].get("cache_hit"):
                if result["log"].get("data_source") == "supabase":
                    diamond_db_hits += 1
                else:
                    diamond_cache_hits += 1
            else:
                diamond_call_secs.append(float(result["log"].get("duration_sec") or 0))
            if result["error"]:
                diamond_errors[key] = result["error"]
            elif result["summary"] is not None:
                diamond_results[key] = result["summary"]

        t_parallel = time.time()
        with ThreadPoolExecutor(max_workers=pool_workers) as executor:
            prep_futures: Dict[Any, str] = {
                executor.submit(_timed_estimates, val_d): "wp",
                executor.submit(_timed_estimates, prior_d): "wp_prior",
            }
            if use_psc:
                prep_futures[executor.submit(_timed_psc)] = "psc"
            close_futures = {
                executor.submit(_fetch_nav_task, task): task for task in diamond_close_tasks
            }

            for future in as_completed({**prep_futures, **close_futures}):
                if future in close_futures:
                    _apply_diamond_result(future.result())
                    continue
                kind = prep_futures[future]
                if kind == "wp":
                    wp, compliance_sec = future.result()
                elif kind == "wp_prior":
                    wp_prior, compliance_prior_sec = future.result()
                elif kind == "psc":
                    psc_navs, psc_sec = future.result()

            # Prior-day sheets: second phase (dynamic submit inside as_completed never ran these).
            prior_tasks: List[tuple] = []
            if include_prior_diamond and not diamond_short_circuit:
                for idx, spec in enumerate(fund_specs):
                    if (idx, "close") in diamond_errors:
                        continue
                    if diamond_results.get((idx, "close")) is not None:
                        prior_tasks.append((idx, spec["id"], prior_date, "open"))

            if prior_tasks:
                prior_futures = {
                    executor.submit(_fetch_nav_task, task): task for task in prior_tasks
                }
                for future in as_completed(prior_futures):
                    _apply_diamond_result(future.result())
        parallel_wall_sec = time.time() - t_parallel
        diamond_wall_sec = max(diamond_call_secs) if diamond_call_secs else 0.0
        diamond_avg_sec = (
            sum(diamond_call_secs) / len(diamond_call_secs) if diamond_call_secs else 0.0
        )

        include_psc_boxed = os.environ.get("SGGG_NAV_CHECKER_PSC_BOXED", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        boxed_by_fund: Dict[str, List[Dict[str, Any]]] = {}
        psc_posn_dates_by_fund: Dict[str, str] = {}
        psc_boxed_sec = 0.0
        psc_boxed_error: Optional[str] = None
        if include_psc_boxed:
            t_psc_boxed = time.time()
            try:
                (
                    boxed_by_fund,
                    positions_by_fund,
                    psc_boxed_error,
                    psc_posn_dates_by_fund,
                ) = fetch_boxed_positions_for_funds(
                    fund_specs,
                    valuation_date,
                    store_portfolios=True,
                    dsn=(os.environ.get("SGGG_PSC_ODBC_DSN") or "PSC_VIEWER").strip() or "PSC_VIEWER",
                )
            except Exception as exc:
                psc_boxed_error = str(exc)
            psc_boxed_sec = time.time() - t_psc_boxed

        entries = [_base_entry(spec, wp, wp_prior, psc_navs) for spec in fund_specs]

        for idx, entry in enumerate(entries):
            est_row = entry.pop("_est_row", {})
            est_prior = entry.pop("_est_prior", {})
            estimate_bps_from_compliance = entry.pop("_estimate_bps_from_compliance", False)
            summary_close = diamond_results.get((idx, "close"))
            close_err = diamond_errors.get((idx, "close"))
            if close_err and not summary_close:
                if isinstance(close_err, DiamondNavUnavailableError):
                    entry["status"] = "unavailable"
                    entry["message"] = close_err.user_message
                else:
                    entry["status"] = "error"
                    entry["error"] = str(close_err)
                entry["boxed_positions"] = _boxed_positions_for_fund_id(
                    boxed_by_fund, entry.get("fund_id") or ""
                )
                _psc_actual = psc_posn_dates_by_fund.get(entry.get("fund_id") or "")
                if _psc_actual:
                    entry["psc_posn_date_actual"] = _psc_actual
                results[idx] = entry
                continue

            if summary_close:
                entry["classes"] = enrich_classes_display_labels(
                    summary_close.get("classes") or [],
                    entry.get("fund_id"),
                )
                entry["class_nav_source"] = "diamond"
                entry["class_i_bps"] = pick_class_i_bps(entry["classes"])
                entry["diamond_nav_requested_date"] = valuation_date
                sheet_date = normalize_diamond_sheet_date(summary_close.get("valuation_date"))
                entry["diamond_nav_sheet_date"] = sheet_date
                if sheet_date and sheet_date != valuation_date:
                    entry["diamond_date_warning"] = (
                        f"Diamond NAV sheet is dated {sheet_date}; requested {valuation_date}"
                    )
                closing_nav = summary_close.get("fund_aum_closing")
                if closing_nav is None:
                    closing_nav = summary_close.get("net_asset_value_native")
                if closing_nav is None:
                    closing_nav = summary_close.get("net_asset_value")
                if closing_nav is not None:
                    entry["diamond_closing_aum"] = float(closing_nav)
                if summary_close.get("native_currency"):
                    entry["diamond_aum_currency"] = summary_close["native_currency"]
                entry["diamond_capital_flow_candidates"] = summary_close.get(
                    "capital_flow_candidates"
                )

            summary_open = diamond_results.get((idx, "open"))
            open_err = diamond_errors.get((idx, "open"))
            prior_norm = normalize_valuation_date(prior_date)
            opening_nav: Optional[float] = None
            if summary_open:
                effective_prior = (
                    summary_open.get("prior_effective_valuation_date") or prior_norm
                )
                entry["diamond_prior_valuation_date"] = effective_prior
                if effective_prior != prior_norm:
                    entry["prior_valuation_date"] = effective_prior
                open_sheet_date = normalize_diamond_sheet_date(
                    summary_open.get("sheet_valuation_date")
                    or summary_open.get("valuation_date")
                )
                if summary_open.get("prior_holiday_fallback"):
                    warn = (
                        f"Prior business day {prior_date} had no Diamond NAV; "
                        f"using sheet dated {effective_prior} for opening AUM."
                    )
                    entry["diamond_date_warning"] = (
                        f"{entry['diamond_date_warning']}; {warn}"
                        if entry.get("diamond_date_warning")
                        else warn
                    )
                elif open_sheet_date and open_sheet_date != effective_prior:
                    warn = (
                        f"Prior GetNAVSheet requested {effective_prior} but sheet is dated "
                        f"{open_sheet_date}; prior-day flows may be wrong."
                    )
                    entry["diamond_date_warning"] = (
                        f"{entry['diamond_date_warning']}; {warn}"
                        if entry.get("diamond_date_warning")
                        else warn
                    )
                opening_nav, prior_eod, prior_flow, open_src = sggg_opening_aum_from_prior_summary(
                    summary_open, effective_prior
                )
                if opening_nav is not None:
                    entry["diamond_opening_aum"] = opening_nav
                    entry["diamond_prior_eod_aum"] = prior_eod
                    entry["diamond_prior_day_flow_adjustment"] = prior_flow
                    entry["diamond_opening_aum_source"] = open_src
                if summary_open.get("native_currency") and not summary_close:
                    entry["diamond_aum_currency"] = summary_open["native_currency"]

            if summary_open and summary_open.get("diamond_aum_components"):
                entry["diamond_open_aum_components"] = summary_open["diamond_aum_components"]
            if summary_close and summary_close.get("diamond_aum_components"):
                entry["diamond_close_aum_components"] = summary_close["diamond_aum_components"]

            if summary_close:
                entry["aum_parse_version"] = summary_close.get("aum_parse_version")

            # SGGG: opening = prior-day EOD only; report-day subs/reds subtracted from raw close − open.
            diamond_capital_flow: Optional[float] = None
            report_flow_label: Optional[str] = None
            if summary_close:
                diamond_capital_flow, report_flow_label = capital_flow_net_from_summary(
                    summary_close,
                    opening_equity_only=True,
                )
                if diamond_capital_flow is not None:
                    # Amount still adjusts SGGG day change; omit verbose opening-equity label in UI.
                    report_flow_label = None
                else:
                    diamond_capital_flow, report_flow_label = capital_flow_net_from_summary(
                        summary_close,
                        opening_equity_only=False,
                    )
                if report_flow_label:
                    entry["capital_flow_adjustment_label"] = (
                        f"report day {valuation_date}: {report_flow_label}"
                    )

            if entry["diamond_opening_aum"] is not None and entry["diamond_closing_aum"] is not None:
                raw_change = entry["diamond_closing_aum"] - entry["diamond_opening_aum"]
                entry["sggg_nav_change_raw"] = raw_change
                if diamond_capital_flow is not None:
                    entry["capital_flow_adjustment"] = diamond_capital_flow
                    entry["capital_flow_adjustment_source"] = "diamond"
                    entry["sggg_nav_change_dollars"] = raw_change - diamond_capital_flow
                else:
                    entry["sggg_nav_change_dollars"] = raw_change
                    if abs(raw_change) > 100_000:
                        entry["sggg_nav_change_note"] = (
                            "Diamond NAV sheet has no subs/reds lines parsed; "
                            "SGGG day change is unadjusted (compliance AF is not applied to SGGG)."
                        )
                entry["sggg_nav_change_source"] = "diamond"
            else:
                missing: List[str] = []
                if entry["diamond_opening_aum"] is None:
                    if isinstance(open_err, DiamondNavUnavailableError):
                        missing.append(open_err.user_message)
                    elif summary_open and fund_aum_from_summary(summary_open) is None:
                        missing.append(
                            f"prior day ({prior_date}): Diamond sheet returned no fund-level AUM"
                        )
                    else:
                        missing.append(f"prior day ({prior_date})")
                if entry["diamond_closing_aum"] is None:
                    if isinstance(close_err, DiamondNavUnavailableError):
                        missing.append(close_err.user_message)
                    else:
                        missing.append(f"report day ({valuation_date})")
                if missing:
                    entry["sggg_nav_change_note"] = (
                        "Diamond fund AUM not available yet: " + "; ".join(missing)
                    )

            if entry.get("estimate_bps") is not None and entry.get("opening_nav_aum") is not None:
                entry["estimate_nav_change_dollars"] = float(entry["opening_nav_aum"]) * (
                    float(entry["estimate_bps"]) / 10_000.0
                )

            if not estimate_bps_from_compliance:
                prior_eod = est_row.get("prior_eod_aum") or entry.get("opening_nav_aum")
                if (
                    prior_eod is not None
                    and entry.get("estimate_nav_change_dollars") is not None
                    and float(prior_eod) != 0
                ):
                    entry["estimate_bps"] = int(
                        round(
                            (float(entry["estimate_nav_change_dollars"]) / float(prior_eod)) * 10_000
                        )
                    )
            if entry["class_i_bps"] is not None and entry["estimate_bps"] is not None:
                entry["bps_difference"] = int(entry["class_i_bps"]) - int(entry["estimate_bps"])
            if entry["sggg_nav_change_dollars"] is not None and entry["estimate_nav_change_dollars"] is not None:
                entry["nav_change_difference_dollars"] = (
                    float(entry["sggg_nav_change_dollars"]) - float(entry["estimate_nav_change_dollars"])
                )

            if summary_close:
                entry["status"] = "available" if summary_close.get("available") else "unavailable"
            entry["boxed_positions"] = _boxed_positions_for_fund_id(
                boxed_by_fund, entry.get("fund_id") or ""
            )
            _psc_actual = psc_posn_dates_by_fund.get(entry.get("fund_id") or "")
            if _psc_actual:
                entry["psc_posn_date_actual"] = _psc_actual
            results[idx] = entry

        elapsed = time.time() - started
        bottleneck_hint = None
        if diamond_skip_reason == "early_today":
            bottleneck_hint = (
                "Skipped Diamond before 4:30pm Eastern for today's date. "
                "Compliance estimates still load; check Force SGGG check after NAV release."
            )
        elif parallel_wall_sec > diamond_wall_sec + 2:
            if max(compliance_sec, compliance_prior_sec) >= diamond_wall_sec:
                bottleneck_hint = (
                    "Compliance workbook reads on P: (two dates) dominated wall time; "
                    "Diamond calls now run in parallel with those reads."
                )
            elif use_psc and psc_sec >= diamond_wall_sec:
                bottleneck_hint = "PSC ODBC queries dominated wall time (disable with SGGG_NAV_CHECKER_USE_PSC=0)."
        if not bottleneck_hint and diamond_avg_sec >= 12:
            bottleneck_hint = (
                f"Diamond GetNAVSheet averaged {diamond_avg_sec:.1f}s per call "
                f"(slowest {diamond_wall_sec:.1f}s) even when NAV is not ready — SGGG server latency."
            )

        logging.getLogger(__name__).info(
            "nav-availability: %d funds, %d diamond calls in %.2fs "
            "(parallel=%.2fs compliance=%.2fs/%.2fs diamond_wall=%.2fs workers=%d psc=%s)",
            len(fund_specs),
            len(diamond_call_secs),
            elapsed,
            parallel_wall_sec,
            compliance_sec,
            compliance_prior_sec,
            diamond_wall_sec,
            diamond_workers,
            use_psc,
        )

        compliance_meta = {
            "available": wp.get("available", False),
            "workbook_path": wp.get("workbook_path"),
            "note": wp.get("note"),
            "error": wp.get("error"),
            "file_variant": wp.get("file_variant"),
            "saved_at": wp.get("saved_at"),
            "sheet_as_of": wp.get("sheet_as_of"),
            "date_warning": wp.get("date_warning"),
        }
        sorted_calls = sorted(
            diamond_calls_detail,
            key=lambda r: (0 if r.get("role") == "close" else 1, r.get("fund_index", 0)),
        )
        for call_no, row in enumerate(sorted_calls, start=1):
            row["call_number"] = call_no
        diamond_escalation = {
            "client": "EH Partners / EHP Fund Admin NAV checker (DataBridge)",
            "api_base_url": diamond_api_base,
            "auth": {
                "method": "POST",
                "url": f"{diamond_api_base}/login/",
                "request_body": {"Username": "<SGGG_DIAMOND_USERNAME>", "Password": "<redacted>"},
                "note": "One login at start of each checker run; AuthKey reused for all GetNAVSheet calls (~1h TTL).",
            },
            "valuation_date": valuation_date,
            "prior_business_day": prior_date,
            "execution_model": (
                f"{len(diamond_call_secs)} live GetNAVSheet POST(s), {diamond_db_hits} from Supabase, "
                f"{diamond_cache_hits} in-memory (max_workers={diamond_workers}); wall clock ≈ slowest live call. "
                "Successful sheets persist in fund_admin_diamond_nav_snapshots; use force refresh to bypass."
            ),
            "calls": sorted_calls,
        }
        return jsonify(
            {
                "valuation_date": valuation_date,
                "prior_valuation_date": prior_date,
                "compliance_check": compliance_meta,
                "working_paper": compliance_meta,
                "funds": results,
                "diamond_calls_detail": sorted_calls,
                "diamond_escalation": diamond_escalation,
                "timing": {
                    "nav_checker_build": "sggg-psc-boxed-v17",
                    "psc_boxed_total": sum(len(v or []) for v in boxed_by_fund.values())
                    if include_psc_boxed
                    else None,
                    "psc_boxed_fund_counts": {
                        spec["id"]: len(_boxed_positions_for_fund_id(boxed_by_fund, spec["id"]))
                        for spec in fund_specs
                    }
                    if include_psc_boxed
                    else None,
                    "total_sec": round(elapsed, 2),
                    "parallel_wall_sec": round(parallel_wall_sec, 2),
                    "compliance_sec": round(compliance_sec, 2),
                    "compliance_prior_sec": round(compliance_prior_sec, 2),
                    "psc_sec": round(psc_sec, 2) if use_psc else None,
                    "psc_boxed_sec": round(psc_boxed_sec, 2) if include_psc_boxed else None,
                    "psc_boxed_error": psc_boxed_error,
                    "psc_enabled": use_psc,
                    "diamond_wall_sec": round(diamond_wall_sec, 2),
                    "diamond_slowest_sec": round(diamond_wall_sec, 2),
                    "diamond_avg_sec": round(diamond_avg_sec, 2),
                    "diamond_requests": len(diamond_call_secs),
                    "diamond_cache_hits": diamond_cache_hits,
                    "diamond_db_hits": diamond_db_hits,
                    "diamond_requests_live": len(diamond_call_secs),
                    "force_diamond": force_diamond,
                    "diamond_requests_max": len(fund_specs)
                    * (2 if include_prior_diamond and not diamond_short_circuit else 1),
                    "diamond_skipped": diamond_skip_reason is not None,
                    "diamond_skip_reason": diamond_skip_reason,
                    "diamond_workers": diamond_workers,
                    "bottleneck_hint": bottleneck_hint,
                },
            }
        )
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


def _port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """
    Best-effort check to prevent accidentally running multiple DataBridge instances.
    Returns True if we cannot bind the (host, port) tuple.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Avoid keeping the port reserved; we just probe.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        s.bind((host, int(port)))
        return False
    except OSError:
        return True
    finally:
        try:
            s.close()
        except Exception:
            pass


def _listening_pids_windows(port: int) -> List[int]:
    """Return PIDs listening on 127.0.0.1:port (Windows-only, best-effort)."""
    try:
        out = subprocess.check_output(["netstat", "-ano", "-p", "tcp"], text=True, errors="replace")
    except Exception:
        return []

    pids: List[int] = []
    needle = f":{int(port)}"
    for line in out.splitlines():
        # Example: TCP  127.0.0.1:5000  0.0.0.0:0  LISTENING  71832
        if "LISTENING" not in line:
            continue
        if needle not in line:
            continue
        parts = line.split()
        if not parts:
            continue
        try:
            pid = int(parts[-1])
        except Exception:
            continue
        pids.append(pid)

    # De-dupe while keeping order.
    seen = set()
    deduped: List[int] = []
    for pid in pids:
        if pid in seen:
            continue
        seen.add(pid)
        deduped.append(pid)
    return deduped


if __name__ == "__main__":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True)
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass

    # Guard: avoid running multiple listeners on the same port.
    if _port_in_use(SERVICE_PORT, host="127.0.0.1"):
        pids = _listening_pids_windows(SERVICE_PORT)
        pid_hint = f" (PID(s): {', '.join(str(p) for p in pids)})" if pids else ""
        print(f"[FATAL] Port {SERVICE_PORT} is already in use{pid_hint}.", file=sys.stderr)
        print("Stop the existing DataBridge process before starting another.", file=sys.stderr)
        sys.exit(1)
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

