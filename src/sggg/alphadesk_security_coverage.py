"""Read-only AlphaDesk security-master coverage checks for production runs."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

MAX_COVERAGE_SECURITIES = 500
_SEDOL_PATTERN = re.compile(r"[0-9BCDFGHJKLMNPQRSTVWXYZ]{6}[0-9]")
_SEDOL_WEIGHTS = (1, 3, 1, 7, 3, 9, 1)
_FIELDS = (
    "SECURITY",
    "DESCRIPTION",
    "SEC_CCY",
    "CUSIP",
    "ISIN",
    "SEDOL",
    "BBG_TICKER",
    "COMPANY_SYMBOL",
    "COUNTRY",
    "EXCHANGE",
)


def _normalized(value: Any) -> str:
    return str(value or "").strip().upper()


def _sedol_character_value(character: str) -> int:
    return int(character) if character.isdigit() else ord(character) - ord("A") + 10


def is_valid_sedol(value: Any) -> bool:
    """Return whether a value is a format- and checksum-valid SEDOL."""
    sedol = _normalized(value)
    return bool(_SEDOL_PATTERN.fullmatch(sedol)) and sum(
        _sedol_character_value(character) * weight
        for character, weight in zip(sedol, _SEDOL_WEIGHTS, strict=True)
    ) % 10 == 0


def _request_values(request: Mapping[str, Any], field: str) -> set[str]:
    value = _normalized(request.get(field))
    if not value:
        return set()
    if field == "symbol":
        base = value.split(".", 1)[0]
        return {value, base}
    if field == "bloomberg_symbol":
        return {value, value.split()[0]}
    return {value}


def _matching_score(request: Mapping[str, Any], row: Mapping[str, str]) -> int | None:
    comparisons = (
        ("cusip", "CUSIP", 0),
        ("isin", "ISIN", 1),
        ("sedol", "SEDOL", 2),
        ("bloomberg_symbol", "BBG_TICKER", 3),
    )
    for request_field, row_field, score in comparisons:
        if _request_values(request, request_field) & {_normalized(row.get(row_field))}:
            return score
    symbols = _request_values(request, "symbol")
    row_symbols = {
        _normalized(row.get("COMPANY_SYMBOL")),
        _normalized(row.get("SECURITY")),
        _normalized(row.get("BBG_TICKER")).split()[0],
    }
    return 4 if symbols & row_symbols else None


def _normalized_requests(requests: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for request in requests:
        record = {
            key: str(request.get(key) or "").strip()
            for key in ("symbol", "bloomberg_symbol", "cusip", "isin", "sedol")
        }
        if not record["symbol"]:
            raise ValueError("Every coverage request requires a symbol.")
        if any(len(value) > 100 for value in record.values()):
            raise ValueError(f"Security identifiers are too long for {record['symbol']}.")
        normalized.append(record)
    if not normalized:
        return []
    if len(normalized) > MAX_COVERAGE_SECURITIES:
        raise ValueError(
            f"AlphaDesk coverage accepts at most {MAX_COVERAGE_SECURITIES} securities."
        )
    return normalized


def _query_rows(connection: Any, requests: list[dict[str, str]]) -> list[dict[str, str]]:
    values_by_column: dict[str, set[str]] = {
        "CUSIP": set(),
        "ISIN": set(),
        "SEDOL": set(),
        "BBG_TICKER": set(),
        "COMPANY_SYMBOL": set(),
        "SECURITY": set(),
    }
    for request in requests:
        values_by_column["CUSIP"].update(_request_values(request, "cusip"))
        values_by_column["ISIN"].update(_request_values(request, "isin"))
        values_by_column["SEDOL"].update(_request_values(request, "sedol"))
        values_by_column["BBG_TICKER"].update(
            _request_values(request, "bloomberg_symbol")
        )
        symbol_values = _request_values(request, "symbol")
        values_by_column["COMPANY_SYMBOL"].update(symbol_values)
        values_by_column["SECURITY"].update(symbol_values)

    clauses: list[str] = []
    parameters: list[str] = []
    for column, values in values_by_column.items():
        if not values:
            continue
        ordered = sorted(values)
        clauses.append(
            f"UPPER(TRIM(COALESCE({column}, ''))) IN "
            f"({','.join('?' for _ in ordered)})"
        )
        parameters.extend(ordered)
    if not clauses:
        return []
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT {','.join(_FIELDS)} FROM psc_security_data WHERE "
        + " OR ".join(clauses),
        tuple(parameters),
    )
    return [
        {
            field: str(value or "").strip()
            for field, value in zip(_FIELDS, row, strict=True)
        }
        for row in cursor.fetchall()
    ]


def fetch_alphadesk_security_coverage(
    connection: Any,
    requested_securities: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Classify requested securities against AlphaDesk and its stored SEDOLs."""
    requests = _normalized_requests(requested_securities)
    matched_rows = _query_rows(connection, requests)
    results: list[dict[str, Any]] = []
    for request in requests:
        candidates = [
            (score, row)
            for row in matched_rows
            if (score := _matching_score(request, row)) is not None
        ]
        candidates.sort(
            key=lambda candidate: (
                candidate[0],
                not is_valid_sedol(candidate[1].get("SEDOL")),
                _normalized(candidate[1].get("COMPANY_SYMBOL")),
            )
        )
        best = candidates[0][1] if candidates else None
        if best is None:
            status = "missing_security"
        elif is_valid_sedol(best.get("SEDOL")):
            status = "ready"
        else:
            status = "missing_or_invalid_sedol"
        results.append(
            {
                "symbol": request["symbol"],
                "status": status,
                "alphadesk_symbol": best.get("COMPANY_SYMBOL") if best else None,
                "alphadesk_bloomberg_symbol": best.get("BBG_TICKER") if best else None,
                "alphadesk_sedol": best.get("SEDOL") if best else None,
            }
        )
    return {
        "securities": results,
        "requested_count": len(results),
        "ready_count": sum(result["status"] == "ready" for result in results),
    }
