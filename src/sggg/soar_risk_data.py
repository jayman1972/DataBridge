"""Independent SOAR risk-report data collection from Diamond, PSC and Bloomberg.

The legacy TXT is used only for model history through 2026-10-16.  User-supplied
returns bridge 2026-10-19..23; later observations come from the production
reference-portfolio schedule.  Excel is not read or executed at runtime.
"""

from __future__ import annotations

import csv
import math
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sggg.diamond_client import DiamondNavUnavailableError
from sggg.nav_sheet_parse import parse_nav_sheet_summary
from sggg.soar_risk_math import (
    growth_series,
    historical_var_95,
    merge_model_returns,
    parse_iso_date,
    prior_month_end,
    prior_quarter_end,
    prior_year_end,
    week_start,
    weighted_scenario_return,
)

SOAR_PARENT_ID = "41010000-7f79-9c86-0bd2-45484608d9e1"
CLASS_B_ID = "950B"
SOAR_REFERENCE_SCHEDULE_ID = "2abee65c-d978-49da-9f56-161b596a0d6e"
SOAR_STRATEGY_ID = "cd5d74f2-1a74-44ce-9497-275a4dfd5256"
MODEL_HANDOFF_DATE = date(2026, 10, 23)
MODEL_LEGACY_LAST_DATE = date(2026, 10, 16)
INCEPTION_DATE = date(2022, 2, 1)
CHART_START_DATE = date(2022, 9, 14)

# Distribution-adjusted inception base from the 2026-10-23 reference workbook.
# Future valuation-period total returns are chained from that audited handoff.
CLASS_B_INCEPTION_ADJUSTED_NAVPU = 8.30368
CLASS_B_HANDOFF_NAVPU = 12.9231

MODEL_TXT_PATH = Path(
    os.environ.get(
        "SOAR_LEGACY_MODEL_RETURNS_PATH",
        r"\\ehp04\workgroups\James Park\EHP Funds\The SOAR Fund\Backtest Results\TheSOARFund-EquityLS-US-Last-returns.TXT",
    )
)
MODEL_GAP_PATH = Path(__file__).resolve().parent / "data" / "soar_model_gap_20261019_20261023.csv"

SCENARIOS = (
    ("Covid-19 Sell Off", date(2020, 2, 3), date(2020, 4, 30)),
    ("Equity Markets Rebound in 2009", date(2009, 3, 4), date(2009, 6, 1)),
    ("Lehman Default", date(2008, 9, 15), date(2008, 10, 14)),
    ("Sept 11, 2001", date(2001, 9, 5), date(2001, 9, 26)),
)


class DiamondValuationDateMismatch(ValueError):
    """The requested date maps to a different Diamond valuation strike."""


def _number(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Missing or invalid {label}") from exc
    if not math.isfinite(result):
        raise ValueError(f"Missing or invalid {label}")
    return result


def _compact_date(value: object) -> date:
    text = str(value)
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError("PSC date must be YYYYMMDD")
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _class_b_nav(client: Any, target: date) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = client.get_nav_sheet(SOAR_PARENT_ID, target.isoformat())
    summary = parse_nav_sheet_summary(raw)
    if not summary.get("available"):
        raise ValueError(f"Diamond Class B NAV is unavailable for {target.isoformat()}")
    actual_date = parse_iso_date(summary.get("sheet_valuation_date"))
    if actual_date != target:
        raise DiamondValuationDateMismatch(
            f"Diamond returned {actual_date.isoformat()} for requested {target.isoformat()}"
        )
    if str(summary.get("fund_parent_id", "")).lower() != SOAR_PARENT_ID:
        raise ValueError("Diamond returned the wrong parent fund")
    if summary.get("fund_currency") != "CAD":
        raise ValueError("Diamond returned an unexpected fund currency")
    classes = [row for row in summary.get("classes", []) if row.get("class_id") == CLASS_B_ID]
    if len(classes) != 1 or classes[0].get("nav_currency") != "CAD":
        raise ValueError("Diamond Class B (950B) CAD NAV per share is missing")
    navpu = _number(classes[0].get("navpu"), "Class B NAV per share")
    if navpu <= 0:
        raise ValueError("Class B NAV per share must be positive")
    return summary, raw


def _tsx_closed_on_friday(bbg: Any, friday: date) -> bool:
    """Confirm a Canadian-market Friday holiday from nearby TSX closes."""
    rows = bbg.get_historical_data(
        ticker="SPTSX Index", fields=["PX_LAST"],
        start_date=(friday - timedelta(days=7)).isoformat(),
        end_date=friday.isoformat(), adjustment_profile="raw",
    )
    observed = {parse_iso_date(row.get("date")) for row in rows}
    return friday not in observed and any(
        friday - timedelta(days=4) <= day < friday for day in observed
    )


def weekly_position_date(client: Any, week_ending: date, bbg: Any = None) -> date:
    """Use the week's final exact NAV strike, never a later month-end strike.

    An unfinalized Friday is treated as a holiday only when nearby Bloomberg
    TSX closes confirm that Friday's market was shut. Otherwise, retry rather
    than pick an unrelated midweek/month-end NAV strike.
    """
    if week_ending.weekday() != 4:
        raise ValueError("Week-ending date must be a Friday")
    confirmed_holiday = False
    for offset in range(5):
        candidate = week_ending - timedelta(days=offset)
        try:
            _class_b_nav(client, candidate)
        except DiamondValuationDateMismatch:
            continue
        except DiamondNavUnavailableError:
            if offset == 0:
                if bbg is None or not _tsx_closed_on_friday(bbg, week_ending):
                    raise
                confirmed_holiday = True
            elif not confirmed_holiday:
                raise
            continue
        return candidate
    raise ValueError(f"No exact weekly SOAR NAV strike for week ending {week_ending}")


def _nav_at_or_before(client: Any, target: date) -> tuple[date, float]:
    """Find a valuation on/before an anchor; Diamond may return a later sheet."""
    for offset in range(8):
        candidate = target - timedelta(days=offset)
        try:
            summary, _ = _class_b_nav(client, candidate)
        except ValueError:
            continue
        return candidate, _number(summary["classes"][0]["navpu"], "anchor NAVPU")
    raise ValueError(f"No Class B NAV per share at or before {target.isoformat()}")


def _class_b_item(raw: dict[str, Any], name: str) -> str | None:
    body = raw.get("GetNAVSheetResponse", raw)
    if not isinstance(body, dict):
        return None
    for cls in body.get("ClassSeriesFundList") or []:
        if cls.get("FundID") != CLASS_B_ID:
            continue
        for section in cls.get("SectionList") or []:
            for item in section.get("SectionItem") or []:
                if item.get("Name") == name:
                    return str(item.get("Value") or "").strip()
    return None


def _valuation_dates_after_handoff(client: Any, bbg: Any, as_of: date) -> list[date]:
    days: set[date] = {as_of}
    friday = MODEL_HANDOFF_DATE + timedelta(days=7)
    while friday <= as_of:
        days.add(weekly_position_date(client, friday, bbg))
        friday += timedelta(days=7)
    month_end = prior_month_end(MODEL_HANDOFF_DATE.replace(day=1) + timedelta(days=35))
    while month_end <= as_of:
        try:
            _class_b_nav(client, month_end)
        except DiamondValuationDateMismatch:
            pass  # No separate month-end strike on this calendar date.
        else:
            days.add(month_end)  # Include intervening returns, not an extra report.
        month_end = prior_month_end(month_end + timedelta(days=35))
    return sorted(day for day in days if day > MODEL_HANDOFF_DATE)


def _since_inception_annualized(client: Any, bbg: Any, as_of: date) -> float:
    if as_of < MODEL_HANDOFF_DATE:
        raise ValueError("Reports before the audited 2026-10-23 handoff are unsupported")
    multiplier = CLASS_B_HANDOFF_NAVPU / CLASS_B_INCEPTION_ADJUSTED_NAVPU
    prior_navpu = CLASS_B_HANDOFF_NAVPU
    for day in _valuation_dates_after_handoff(client, bbg, as_of):
        summary, raw = _class_b_nav(client, day)
        previous_text = _class_b_item(raw, "Previous Valuation NAVPU")
        previous = _number(previous_text, "previous Class B NAVPU")
        if abs(previous - prior_navpu) > 0.0002:
            raise ValueError(
                f"Class B total-return chain has a gap before {day.isoformat()}"
            )
        raw_return = _class_b_item(raw, "Valuation Period Return")
        if not raw_return or not raw_return.endswith("%"):
            raise ValueError(f"Class B total return is missing on {day.isoformat()}")
        period_return = _number(raw_return[:-1], "valuation-period return") / 100
        multiplier *= 1 + period_return
        prior_navpu = _number(summary["classes"][0]["navpu"], "Class B NAVPU")
    years = (as_of - INCEPTION_DATE).days / 365
    return multiplier ** (1 / years) - 1


def _psc_daily_returns(connection: Any, as_of: date) -> dict[date, float]:
    query = """
        SELECT POSN_DATE_INT,
               MIN(PREV_DAY_NAV), MAX(PREV_DAY_NAV),
               MIN(PORTFOLIO_TRANSFERS), MAX(PORTFOLIO_TRANSFERS),
               SUM(DAY_PROFIT)
        FROM psc_position_history
        WHERE PORTFOLIO = ? AND POSN_DATE_INT BETWEEN ? AND ?
        GROUP BY POSN_DATE_INT ORDER BY POSN_DATE_INT
    """
    rows = connection.cursor().execute(
        query, "The Soar Fund", int(CHART_START_DATE.strftime("%Y%m%d")),
        int(as_of.strftime("%Y%m%d")),
    ).fetchall()
    output: dict[date, float] = {}
    for raw_day, nav_min, nav_max, flow_min, flow_max, pnl in rows:
        day = _compact_date(raw_day)
        if abs(_number(nav_min, "PSC NAV") - _number(nav_max, "PSC NAV")) > 0.01:
            raise ValueError(f"Inconsistent PSC prior NAV on {day.isoformat()}")
        if abs(_number(flow_min, "PSC flows") - _number(flow_max, "PSC flows")) > 0.01:
            raise ValueError(f"Inconsistent PSC transfers on {day.isoformat()}")
        denominator = _number(nav_max, "PSC NAV") + _number(flow_max, "PSC flows")
        if denominator <= 0:
            raise ValueError(f"Invalid PSC return denominator on {day.isoformat()}")
        output[day] = _number(pnl, "PSC P&L") / denominator
    if not output or max(output) != as_of:
        raise ValueError("PSC daily return history does not reach the report date")
    return output


_FX_PAIR_RE = re.compile(r"^(?:CADUSD|USDCAD)(?:\b|[.\s])", re.IGNORECASE)


def is_fx_trade(symbol: str, description: str = "", bbg_ticker: str = "") -> bool:
    """Exclude cash, CAD/USD spots and forwards from the weekly trade table."""
    symbol = symbol.strip().upper()
    description = description.strip().upper()
    bbg_ticker = bbg_ticker.strip().upper()
    return (
        symbol.startswith("CASH ")
        or bool(_FX_PAIR_RE.match(symbol))
        or "USD/CAD" in description
        or "CAD/USD" in description
        or bbg_ticker.endswith(" CURNCY")
    )


def _psc_weekly_trades(connection: Any, as_of: date) -> list[dict[str, Any]]:
    query = """
        SELECT TRADE_DATE_INT, SECURITY, DESCRIPTION, `ORDER`, FILLED_QTTY,
               PRICE, BBG_TICKER
        FROM psc_filled_orders
        WHERE TRADE_DATE_INT BETWEEN ? AND ? AND ACCOUNT_NAME = ?
        ORDER BY TRADE_DATE_INT, SECURITY_SN
    """
    rows = connection.cursor().execute(
        query, int(week_start(as_of).strftime("%Y%m%d")),
        int(as_of.strftime("%Y%m%d")), "BNS-SOAR LS",
    ).fetchall()
    trades = []
    for raw_day, symbol, description, side, shares, price, ticker in rows:
        symbol_text = str(symbol or "").strip()
        if is_fx_trade(symbol_text, str(description or ""), str(ticker or "")):
            continue
        trades.append(
            {
                "date": _compact_date(raw_day).isoformat(),
                "symbol": symbol_text,
                "description": str(description or "").strip(),
                "side": str(side or "").strip().upper(),
                "shares": _number(shares, "filled quantity"),
                "price": _number(price, "filled price"),
            }
        )
    if len(trades) > 22:
        raise ValueError("More than 22 non-FX trades; the reference page would overflow")
    return trades


def _diamond_holdings(client: Any, as_of: date, nav: float) -> list[dict[str, Any]]:
    response = client.get_portfolio(SOAR_PARENT_ID, as_of.isoformat())
    body = response.get("GetPortfolioResponse") if isinstance(response, dict) else None
    if not isinstance(body, dict) or parse_iso_date(body.get("ValuationDate")) != as_of:
        raise ValueError("Diamond portfolio date does not match the report date")
    holdings = []
    for row in body.get("PortfolioRecordDetails") or []:
        value = _number(row.get("BaseMarketValue"), "position market value")
        if abs(value) < 0.01:
            continue
        ticker = str(row.get("PricingTicker") or "").strip().upper()
        if not re.fullmatch(r"[A-Z0-9.]+ US", ticker):
            raise ValueError(f"Unexpected non-ETF SOAR position: {ticker or 'unknown'}")
        if parse_iso_date(row.get("QuoteDate")) != as_of:
            raise ValueError(f"Stale Diamond price for {ticker}")
        symbol = ticker.split()[0] + ".US"
        holdings.append(
            {
                "symbol": symbol,
                "ticker": ticker + " Equity",
                "name": str(row.get("SecurityName") or ticker).strip(),
                "quantity": _number(row.get("Quantity"), "position quantity"),
                "base_market_value": value,
                "weight": value / nav,
            }
        )
    if not holdings:
        raise ValueError("Diamond returned no priced SOAR ETF positions")
    return sorted(holdings, key=lambda row: abs(row["base_market_value"]), reverse=True)


def _history(client: Any, ticker: str, start: date, end: date, fields: list[str]) -> list[dict[str, Any]]:
    rows = client.get_historical_data(
        ticker=ticker, fields=fields, start_date=start.isoformat(),
        end_date=end.isoformat(), adjustment_profile="raw",
    )
    if not rows:
        raise ValueError(f"Bloomberg returned no history for {ticker}")
    return rows


def _price_at_or_before(rows: list[dict[str, Any]], target: date) -> float:
    eligible = [
        row for row in rows
        if parse_iso_date(row.get("date")) <= target
        and target - parse_iso_date(row.get("date")) <= timedelta(days=7)
    ]
    if not eligible:
        raise ValueError(f"Missing benchmark price at {target.isoformat()}")
    return _number(eligible[-1].get("PX_LAST"), "Bloomberg price")


def _performance(client: Any, bbg: Any, as_of: date, navpu: float) -> list[dict[str, Any]]:
    anchors = (
        ("Last Week", as_of - timedelta(days=7)),
        ("MTD", prior_month_end(as_of)),
        ("QTD", prior_quarter_end(as_of)),
        ("YTD", prior_year_end(as_of)),
    )
    index_rows = {
        ticker: _history(bbg, ticker, INCEPTION_DATE, as_of, ["PX_LAST"])
        for ticker in ("SPTSX Index", "SPX Index")
    }
    current_benchmarks = {
        ticker: _price_at_or_before(rows, as_of) for ticker, rows in index_rows.items()
    }
    output = []
    for label, target in anchors:
        actual_date, anchor_navpu = _nav_at_or_before(client, target)
        output.append(
            {
                "label": label,
                "fund": navpu / anchor_navpu - 1,
                "tsx": current_benchmarks["SPTSX Index"]
                / _price_at_or_before(index_rows["SPTSX Index"], actual_date) - 1,
                "spx": current_benchmarks["SPX Index"]
                / _price_at_or_before(index_rows["SPX Index"], actual_date) - 1,
                "anchor_date": actual_date.isoformat(),
            }
        )
    years = (as_of - INCEPTION_DATE).days / 365
    inception_benchmark = {
        ticker: _price_at_or_before(rows, INCEPTION_DATE)
        for ticker, rows in index_rows.items()
    }
    output.append(
        {
            "label": "S.I. (annl.)",
            "fund": _since_inception_annualized(client, bbg, as_of),
            "tsx": (current_benchmarks["SPTSX Index"] / inception_benchmark["SPTSX Index"]) ** (1 / years) - 1,
            "spx": (current_benchmarks["SPX Index"] / inception_benchmark["SPX Index"]) ** (1 / years) - 1,
            "anchor_date": INCEPTION_DATE.isoformat(),
        }
    )
    return output


def _enrich_holdings(bbg: Any, holdings: list[dict[str, Any]], as_of: date) -> None:
    tickers = [row["ticker"] for row in holdings]
    start = as_of - timedelta(days=365)
    refs = bbg.get_reference_data(
        tickers=tickers,
        fields=["BETA_RAW_OVERRIDABLE"],
        overrides={
            "BETA_OVERRIDE_PERIOD": "D",
            "BETA_OVERRIDE_REL_INDEX": "SPX Index",
            "BETA_OVERRIDE_START_DT": start.strftime("%Y%m%d"),
            "BETA_OVERRIDE_END_DT": as_of.strftime("%Y%m%d"),
        },
    )
    for holding in holdings:
        ticker = holding["ticker"]
        reference = refs.get(ticker) or {}
        beta = _number(reference.get("BETA_RAW_OVERRIDABLE"), f"{ticker} beta")
        holding["beta"] = max(-3.0, min(3.0, beta))
        volume_rows = _history(
            bbg, ticker, as_of - timedelta(days=45), as_of, ["PX_VOLUME"]
        )
        volumes = [
            _number(row.get("PX_VOLUME"), f"{ticker} daily volume")
            for row in volume_rows
            if parse_iso_date(row.get("date")) <= as_of
        ][-20:]
        if len(volumes) < 20 or any(value <= 0 for value in volumes):
            raise ValueError(f"Fewer than 20 volume observations for {ticker}")
        holding["adv20"] = sum(volumes) / len(volumes)
    shorts = [row for row in holdings if row["base_market_value"] < 0]
    if shorts:
        short_refs = bbg.get_reference_data(
            tickers=[row["ticker"] for row in shorts],
            fields=["SHORT_INT_RATIO", "SHORT_INT_PCT_FLT"],
        )
        for holding in shorts:
            reference = short_refs.get(holding["ticker"]) or {}
            holding["short_interest_days"] = _number(
                reference.get("SHORT_INT_RATIO"), f"{holding['ticker']} short interest days"
            )
            holding["short_interest_float"] = _number(
                reference.get("SHORT_INT_PCT_FLT"), f"{holding['ticker']} short interest % float"
            )


def _liquidity(holdings: list[dict[str, Any]], stress: bool) -> dict[str, dict[str, float | int | None]]:
    output = {}
    for label, side in (("long", 1), ("short", -1)):
        rows = [row for row in holdings if row["base_market_value"] * side > 0]
        liquid = [
            row for row in rows
            if abs(row["quantity"]) < row["adv20"] * (0.1 if stress else 1) / 3
        ]
        total_notional = sum(abs(row["base_market_value"]) for row in rows)
        output[label] = {
            "names": len(rows),
            "liquid": len(liquid),
            "pct_names": len(liquid) / len(rows) if rows else None,
            "pct_notional": sum(abs(row["base_market_value"]) for row in liquid)
            / total_notional if total_notional else None,
        }
    return output


def _scenarios(bbg: Any, holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weights = {row["ticker"]: row["weight"] for row in holdings}
    output = []
    for label, start, end in SCENARIOS:
        pairs = {}
        for ticker in weights:
            scenario_ticker = "SOXX US Equity" if ticker == "SMH US Equity" else ticker
            rows = _history(bbg, scenario_ticker, start, end, ["PX_LAST"])
            pairs[ticker] = (
                _price_at_or_before(rows, start),
                _price_at_or_before(rows, end),
            )
        output.append(
            {
                "label": label,
                "return": weighted_scenario_return(weights, pairs),
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        )
    return output


def _read_model_csv(path: Path, date_format: str, return_key: str) -> dict[date, float]:
    if not path.is_file():
        raise ValueError(f"Model return source is missing: {path}")
    result = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            raw_date = str(row["date"])
            if date_format == "%m/%d/%Y":
                month, day_number, year = (int(part) for part in raw_date.split("/"))
                day = date(year, month, day_number)
            else:
                day = parse_iso_date(raw_date)
            value = _number(row.get(return_key), "model return") / 100
            if day in result and abs(result[day] - value) > 0.00001:
                raise ValueError(f"Duplicate model return on {day.isoformat()}")
            result[day] = value
    return result


def _production_model_returns(supabase_client: Any, as_of: date) -> dict[date, float]:
    response = (
        supabase_client.table("bt_reference_portfolio_days")
        .select("result_date,daily_return,source_run_id,strategy_id")
        .eq("schedule_id", SOAR_REFERENCE_SCHEDULE_ID)
        .gt("result_date", MODEL_HANDOFF_DATE.isoformat())
        .lte("result_date", as_of.isoformat())
        .order("result_date")
        .limit(2000)
        .execute()
    )
    output = {}
    for row in response.data or []:
        if row.get("strategy_id") != SOAR_STRATEGY_ID or not row.get("source_run_id"):
            raise ValueError("Production SOAR model row has an unexpected source")
        day = parse_iso_date(row.get("result_date"))
        output[day] = _number(row.get("daily_return"), "production model return")
    return output


def collect_soar_risk_report(
    *, as_of: date, diamond: Any, bbg: Any, supabase_client: Any,
    psc_connection: Any, generated_at: datetime,
) -> dict[str, Any]:
    """Collect a fully sourced report; raise rather than emit partial risk metrics."""
    if as_of < MODEL_HANDOFF_DATE:
        raise ValueError("SOAR report dates before 2026-10-23 are unsupported")
    nav_summary, _ = _class_b_nav(diamond, as_of)
    nav = _number(nav_summary.get("net_asset_value_native"), "Diamond fund NAV")
    if nav <= 0:
        raise ValueError("Diamond fund NAV must be positive")
    navpu = _number(nav_summary["classes"][0]["navpu"], "Class B NAV per share")
    holdings = _diamond_holdings(diamond, as_of, nav)
    _enrich_holdings(bbg, holdings, as_of)
    performance = _performance(diamond, bbg, as_of, navpu)
    actual_returns = _psc_daily_returns(psc_connection, as_of)
    var, var_observations = historical_var_95(actual_returns.items(), as_of)
    legacy = {
        day: value for day, value in _read_model_csv(MODEL_TXT_PATH, "%m/%d/%Y", "return").items()
        if day <= MODEL_LEGACY_LAST_DATE
    }
    gap = _read_model_csv(MODEL_GAP_PATH, "%Y-%m-%d", "return_percent")
    production = _production_model_returns(supabase_client, as_of)
    model_returns = merge_model_returns(legacy, gap, production)
    actual_growth, model_growth, slippage = growth_series(actual_returns, model_returns, as_of)
    longs = [row for row in holdings if row["base_market_value"] > 0]
    shorts = [row for row in holdings if row["base_market_value"] < 0]
    long_notional = sum(row["base_market_value"] for row in longs)
    short_notional = sum(row["base_market_value"] for row in shorts)
    long_beta = sum(row["base_market_value"] * row["beta"] for row in longs)
    short_beta = sum(abs(row["base_market_value"]) * row["beta"] for row in shorts)
    return {
        "fund": "The Soar Fund",
        "position_date": as_of.isoformat(),
        "report_date": generated_at.date().isoformat(),
        "fund_nav": nav,
        "nav_currency": "CAD",
        "class_b_navpu": navpu,
        "class_b_id": CLASS_B_ID,
        "performance": performance,
        "holdings": holdings,
        "trades": _psc_weekly_trades(psc_connection, as_of),
        "trade_week_start": week_start(as_of).isoformat(),
        "exposure": {
            "long": long_notional / nav,
            "short": short_notional / nav,
            "net": (long_notional + short_notional) / nav,
            "gross": (long_notional - short_notional) / nav,
        },
        "beta": {
            "long": long_beta / long_notional if long_notional else None,
            "short": short_beta / abs(short_notional) if short_notional else None,
            "adjusted_long": long_beta / nav,
            "adjusted_short": -short_beta / nav,
            "adjusted_net": (long_beta - short_beta) / nav,
        },
        "liquidity": _liquidity(holdings, stress=False),
        "stressed_liquidity": _liquidity(holdings, stress=True),
        "short_interest": sorted(shorts, key=lambda row: row["short_interest_days"], reverse=True)[:5],
        "short_interest_float": sorted(shorts, key=lambda row: row["short_interest_float"], reverse=True)[:5],
        "var_95_1d": var,
        "var_observations": var_observations,
        "scenarios": _scenarios(bbg, holdings),
        "chart": {
            "actual_growth": [(day.isoformat(), value) for day, value in actual_growth],
            "model_growth": [(day.isoformat(), value) for day, value in model_growth],
            "slippage": [(day.isoformat(), value) for day, value in slippage],
        },
        "sources": {
            "nav": "Diamond GetNAVSheet parent SOAR / Class B 950B",
            "positions": "Diamond GetPortfolio",
            "trades": "PSC BNS-SOAR LS filled orders, non-FX",
            "var": "PSC daily P&L / (prior NAV + transfers), trailing 365 days",
            "model": "Legacy TXT through 2026-10-16; user bridge through 2026-10-23; production reference portfolio thereafter",
        },
    }
