"""Bounded Bloomberg benchmark-weight diagnostics with fail-closed QA.

The desktop BLPAPI entitlement does not expose Bloomberg BQL ``holdings()``.
Benchmark members are therefore never presented as exact ETF holdings unless a
caller explicitly opts into the proxy for diagnostics.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

ETF_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")
BENCHMARK_FIELD = "FUND_BENCHMARK_PRIM"
WEIGHT_FIELD = "INDX_MWEIGHT_HIST"
MAX_ETFS_PER_REQUEST = 25


def normalize_etf_tickers(raw_tickers: object) -> list[str]:
    if not isinstance(raw_tickers, list):
        raise TypeError("etfs must be an array")
    tickers: list[str] = []
    for raw in raw_tickers:
        ticker = str(raw or "").strip().upper()
        if not ETF_PATTERN.fullmatch(ticker):
            raise ValueError(f"Invalid ETF ticker: {ticker or '<blank>'}")
        if ticker not in tickers:
            tickers.append(ticker)
    if not tickers:
        raise ValueError("At least one ETF ticker is required")
    if len(tickers) > MAX_ETFS_PER_REQUEST:
        raise ValueError(f"At most {MAX_ETFS_PER_REQUEST} ETF tickers are allowed")
    return tickers


def next_weekday(source_date: date) -> date:
    usable = source_date + timedelta(days=1)
    while usable.weekday() >= 5:
        usable += timedelta(days=1)
    return usable


def bloomberg_equity_ticker(ticker: str) -> str:
    return f"{ticker} US Equity"


def bloomberg_index_ticker(raw: object) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("Bloomberg primary benchmark is missing")
    return value if value.lower().endswith(" index") else f"{value} Index"


def _row_value(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    normalized = {str(key).strip().lower(): value for key, value in row.items()}
    for name in names:
        if name.lower() in normalized:
            return normalized[name.lower()]
    return None


def normalize_weight_rows(rows: object) -> tuple[list[dict[str, object]], float]:
    if not isinstance(rows, list) or len(rows) < 2:
        raise ValueError("Bloomberg returned fewer than two index constituents")
    parsed: list[tuple[str, float]] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        member = str(
            _row_value(
                raw_row,
                ("Index Member", "Member Ticker and Exchange Code"),
            )
            or ""
        ).strip()
        raw_weight = _row_value(
            raw_row,
            ("Percent Weight", "Percentage Weight", "Actual Weight", "Weight"),
        )
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError):
            continue
        if not member or not (weight >= 0):
            continue
        parsed.append((member, weight))
    if len(parsed) < 2:
        raise ValueError("Bloomberg returned no usable non-negative constituent weights")

    raw_sum = sum(weight for _, weight in parsed)
    if 95 <= raw_sum <= 105:
        scale = 100.0
    elif 0.95 <= raw_sum <= 1.05:
        scale = 1.0
    else:
        raise ValueError(
            f"Bloomberg constituent weights failed QA: raw sum {raw_sum:.8f} is not approximately 100%"
        )
    normalized = [
        {"constituentBloombergSymbol": member, "constituentWeight": weight / scale}
        for member, weight in parsed
        if weight > 0
    ]
    normalized_sum = sum(float(row["constituentWeight"]) for row in normalized)
    if len(normalized) < 2 or not 0.95 <= normalized_sum <= 1.05:
        raise ValueError("Bloomberg normalized constituent weights failed QA")
    return normalized, normalized_sum


def fetch_etf_constituent_snapshots(
    bloomberg_client: Any,
    etf_tickers: list[str],
    as_of_date: date,
) -> dict[str, object]:
    equities = [bloomberg_equity_ticker(ticker) for ticker in etf_tickers]
    references = bloomberg_client.get_reference_data(
        tickers=equities,
        fields=[BENCHMARK_FIELD],
    )
    snapshots: list[dict[str, object]] = []
    errors: list[str] = []
    usable_date = next_weekday(as_of_date).isoformat()

    for ticker, equity in zip(etf_tickers, equities, strict=True):
        try:
            reference = references.get(equity) or {}
            if reference.get("error"):
                raise ValueError(str(reference["error"]))
            index_ticker = bloomberg_index_ticker(reference.get(BENCHMARK_FIELD))
            response = bloomberg_client.get_bds_rows(
                index_ticker,
                [WEIGHT_FIELD],
                {"END_DATE_OVERRIDE": as_of_date.strftime("%Y%m%d")},
            )
            response_errors = response.get("errors") or []
            if response_errors:
                raise ValueError("; ".join(str(error) for error in response_errors))
            constituents, weight_sum = normalize_weight_rows(
                (response.get("fields") or {}).get(WEIGHT_FIELD)
            )
            snapshots.append(
                {
                    "etfTicker": ticker,
                    "etfBloombergTicker": equity,
                    "sourceIndexTicker": index_ticker,
                    "sourceReportDate": as_of_date.isoformat(),
                    "usableDate": usable_date,
                    "weightSum": weight_sum,
                    "constituents": constituents,
                }
            )
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors.append(f"{ticker}: {exc}")

    return {
        "sourceProvider": "bloomberg_benchmark_index_proxy",
        "asOfDate": as_of_date.isoformat(),
        "usableDate": usable_date,
        "snapshots": snapshots,
        "errors": errors,
    }
