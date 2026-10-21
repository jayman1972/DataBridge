"""Allowlisted Excel BQL queries and lossless corporate-action table parsing."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timedelta
from typing import Any

from .corporate_actions_lookup import ACTION_FIELDS, summarize, validate_request

BQL_FIELDS = {
    "listings": "listings",
    "delistings": "delistings",
    "ticker_changes": "ticker_change",
}
ALL_ACTION_TYPES = (*ACTION_FIELDS, *BQL_FIELDS)
MAX_QUERY_SECURITIES = 50
MAX_TABLE_ROWS = 10_000
PARTIAL_COVERAGE = (
    "Partial CACT coverage: name-change history and other unverified action families "
    "are not included. Listings, delistings and ticker changes use Excel BQL."
)
SCOPE_COVERAGE = (
    "Issuer and Security Plus expansion use issuer/primary-equity mapping and Related "
    "Equities. They are not verified replicas of Bloomberg CACT's native scope."
)


class PendingBql(RuntimeError):
    """The Bloomberg add-in has not finished producing this query's table."""


def validate_job_options(payload: Any) -> dict[str, Any]:
    return validate_request(payload, allowed_types=ALL_ACTION_TYPES)


def validate_bql_symbols(symbols: list[str]) -> None:
    if not 1 <= len(symbols) <= MAX_QUERY_SECURITIES:
        raise ValueError("Excel BQL requires 1–50 resolved securities per query")
    # Securities are written as text values, never interpolated into BQL or Excel
    # formula text. Reject formula-like and control characters as defense in depth.
    if any(
        not isinstance(symbol, str)
        or not re.fullmatch(r"[A-Za-z0-9/][A-Za-z0-9 ./_@:+-]{0,199}", symbol)
        for symbol in symbols
    ):
        raise ValueError(
            "Bloomberg returned a security identifier unsafe for Excel input"
        )


def build_formula(kind: str, options: dict[str, Any], count: int) -> str:
    if kind not in BQL_FIELDS or not 1 <= count <= MAX_QUERY_SECURITIES:
        raise ValueError("Unsupported Excel corporate-action query")
    basis = options["date_basis"]
    if basis not in {"announced", "effective"}:
        raise ValueError("Invalid date basis")
    start, end = (
        date.fromisoformat(options[key]) for key in ("start_date", "end_date")
    )
    if end < start or (end - start).days > 800:
        raise ValueError("Invalid corporate-action date range")
    item = f"{BQL_FIELDS[kind]}({basis}_date=range({start},{end}))"
    return (
        f'=BQL(BQL.LIST(Inputs!A2:A{count + 1}),"dropna({item}, true)","showallcols=t")'
    )


def json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite Bloomberg value")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ValueError(f"Unexpected Bloomberg cell type: {type(value).__name__}")


def event_date(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(value) or not 1 <= value <= 2_958_465:
            raise ValueError("Invalid Excel event date")
        return (date(1899, 12, 30) + timedelta(days=int(value))).isoformat()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid Bloomberg event date: {str(value)[:80]}") from exc


def parse_table(
    values: Any,
    kind: str,
    options: dict[str, Any],
    universe: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse actual event dates, not BQL's observation/as-of DATES column."""
    if kind not in BQL_FIELDS:
        raise ValueError("Unsupported BQL event type")
    if not values or not isinstance(values, (list, tuple)):
        raise PendingBql("Bloomberg has not returned a table")
    if not isinstance(values[0], (list, tuple)):
        values = [values]
    if len(values) > MAX_TABLE_ROWS:
        raise ValueError("Excel BQL row limit reached; narrow the request")
    populated = [
        value for row in values for value in row if value is not None and value != ""
    ]
    empty_message = (
        f"#N/A No Data: Function dropna({BQL_FIELDS[kind]}("
        f"{options['date_basis']}_date=range({options['start_date']},{options['end_date']})),true) "
        "returned empty results."
    )
    # This exact response was verified against live, valid no-event queries.
    # Generic #N/A, missing values and provider/entitlement errors still fail.
    if populated == [empty_message]:
        return [], []
    for row in values:
        for value in row:
            if isinstance(value, str):
                if any(
                    token in value.lower()
                    for token in (
                        "requesting data",
                        "loading",
                        "#getting_data",
                        "#busy!",
                    )
                ):
                    raise PendingBql("Waiting for Bloomberg Excel data")
                if value.startswith("#"):
                    raise ValueError(f"Bloomberg Excel query error: {value[:300]}")
            elif isinstance(value, int) and -2_146_830_000 < value < -2_146_820_000:
                raise ValueError(f"Bloomberg Excel returned an error cell ({value})")
    headers = [str(value or "").strip() for value in values[0]]
    while headers and not headers[-1]:
        headers.pop()
    required = {"ID", "EFFECTIVE_DATE", "ANNOUNCED_DATE"}
    if not required.issubset(headers):
        raise ValueError(
            "Bloomberg Excel table is missing event date or security columns"
        )
    value_columns = [
        header
        for header in headers
        if re.match(rf"^(?:dropna\(\s*)?{BQL_FIELDS[kind]}\(", header, re.IGNORECASE)
    ]
    if len(value_columns) != 1 or len(set(headers)) != len(headers):
        raise ValueError("Unexpected Bloomberg Excel corporate-action schema")
    canonical = {key.casefold(): key for key in universe}
    events: dict[str, dict[str, Any]] = {}
    warnings = []
    missing_dates = 0
    for values_row in values[1:]:
        if all(value is None or value == "" for value in values_row):
            continue
        if any(
            value is not None and value != "" for value in values_row[len(headers) :]
        ):
            raise ValueError("Bloomberg Excel row extends beyond its header")
        row = {
            key: json_value(value)
            for key, value in zip(headers, values_row, strict=False)
        }
        security = canonical.get(str(row.get("ID", "")).strip().casefold())
        if not security:
            raise ValueError(
                "Bloomberg Excel returned a security outside the requested batch"
            )
        announced = event_date(row.get("ANNOUNCED_DATE"))
        effective = event_date(row.get("EFFECTIVE_DATE"))
        row["ANNOUNCED_DATE"] = announced
        row["EFFECTIVE_DATE"] = effective
        if row.get("DATES") is not None:
            row["DATES"] = event_date(row["DATES"])
        selected = announced if options["date_basis"] == "announced" else effective
        if not selected:
            missing_dates += 1
            continue
        if not options["start_date"] <= selected <= options["end_date"]:
            continue
        # The observed BQL tables do not expose an Action ID. Hash event terms,
        # excluding the observation date and date-window-dependent value header.
        terms = {
            "security": security,
            "kind": kind,
            "announced": announced,
            "effective": effective,
            "value": row.get(value_columns[0]),
            "old_ticker": row.get("OLD_TICKER"),
            "new_ticker": row.get("NEW_TICKER"),
        }
        digest = hashlib.sha256(json.dumps(terms, sort_keys=True).encode()).hexdigest()
        key = f"{kind}:bql:{digest}"
        events[key] = {
            "id": key,
            "security": security,
            "sources": universe[security],
            "matched_securities": [security],
            "action_type": kind,
            "action_id": None,
            "announced_date": announced,
            "effective_date": effective,
            "date": selected,
            "estimated": False,
            "summary": summarize(
                {key: value for key, value in row.items() if key != "DATES"}
            ),
            "details": row,
            "field": f"BQL:{BQL_FIELDS[kind]}",
        }
    if missing_dates:
        warnings.append(
            f"{kind}: {missing_dates} BQL records lack a usable {options['date_basis']} date "
            "and were excluded; this is not evidence of no events."
        )
    return sorted(
        events.values(), key=lambda row: (row["date"], row["security"])
    ), warnings
