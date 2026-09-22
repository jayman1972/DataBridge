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
    return {value}


def _symbol_base(value: Any) -> str:
    return _normalized(value).split(".", 1)[0]


def _security_aliases(value: Any) -> set[str]:
    """Return explicit AlphaDesk listing aliases without losing the market."""
    symbol = _normalized(value)
    if not symbol:
        return set()
    aliases = {symbol}
    if symbol.endswith(".TO"):
        aliases.add(f"{symbol[:-3]}.CA")
    elif symbol.endswith(".CA"):
        aliases.add(f"{symbol[:-3]}.TO")
    elif "." not in symbol:
        aliases.add(f"{symbol}.US")
    return aliases


def _match_details(
    request: Mapping[str, Any], row: Mapping[str, str]
) -> dict[str, Any] | None:
    matched_fields: list[str] = []
    conflicting_fields: list[dict[str, str]] = []

    request_bloomberg = _normalized(request.get("bloomberg_symbol"))
    row_bloomberg = _normalized(row.get("BBG_TICKER"))
    exact_bloomberg = bool(
        request_bloomberg and request_bloomberg == row_bloomberg
    )
    if exact_bloomberg:
        matched_fields.append("bloomberg_symbol")

    exact_security = _normalized(row.get("SECURITY")) in _security_aliases(
        request.get("symbol")
    )
    if exact_security:
        matched_fields.append("security")

    request_base = _symbol_base(request.get("symbol"))
    company_symbol = _normalized(row.get("COMPANY_SYMBOL"))
    company_symbol_match = bool(request_base and request_base == company_symbol)
    if company_symbol_match:
        matched_fields.append("company_symbol")

    identifier_weight = 0
    for request_field, row_field, weight in (
        ("cusip", "CUSIP", 3),
        ("isin", "ISIN", 3),
        ("sedol", "SEDOL", 4),
    ):
        request_value = _normalized(request.get(request_field))
        row_value = _normalized(row.get(row_field))
        if request_value and request_value == row_value:
            matched_fields.append(request_field)
            identifier_weight += weight
        elif row_value and (request_value or request_field == "sedol"):
            # A SEDOL AlphaDesk holds and we lack still conflicts. The trade file
            # carries our SEDOL, so the row would go out blank while the check
            # reported it ready.
            conflicting_fields.append(
                {
                    "field": request_field,
                    "requested": request_value,
                    "alphadesk": row_value,
                }
            )

    if not matched_fields:
        return None
    return {
        "row": row,
        "matched_fields": matched_fields,
        "conflicting_fields": conflicting_fields,
        "exact_bloomberg": exact_bloomberg,
        "exact_security": exact_security,
        "identifier_weight": identifier_weight,
        "company_symbol_match": company_symbol_match,
    }


def _candidate_rank(candidate: Mapping[str, Any]) -> tuple[int, int, int, int]:
    return (
        int(bool(candidate["exact_bloomberg"])),
        int(candidate["identifier_weight"]),
        int(bool(candidate["exact_security"])),
        int(bool(candidate["company_symbol_match"])),
    )


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
        symbol_base = _symbol_base(request.get("symbol"))
        if symbol_base:
            values_by_column["COMPANY_SYMBOL"].add(symbol_base)
        values_by_column["SECURITY"].update(_security_aliases(request.get("symbol")))

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
            details
            for row in matched_rows
            if (details := _match_details(request, row)) is not None
        ]
        candidates.sort(
            key=lambda candidate: (
                *_candidate_rank(candidate),
                _normalized(candidate["row"].get("SECURITY")),
            ),
            reverse=True,
        )
        best_candidate = candidates[0] if candidates else None
        best = best_candidate["row"] if best_candidate else None
        if best is None:
            status = "missing_security"
        elif (
            len(candidates) > 1
            and _candidate_rank(candidates[0]) == _candidate_rank(candidates[1])
            and _normalized(candidates[0]["row"].get("SECURITY"))
            != _normalized(candidates[1]["row"].get("SECURITY"))
        ):
            status = "ambiguous_security"
        elif best_candidate["conflicting_fields"]:
            status = "identity_conflict"
        elif is_valid_sedol(best.get("SEDOL")):
            status = "ready"
        else:
            status = "missing_or_invalid_sedol"
        results.append(
            {
                "symbol": request["symbol"],
                "status": status,
                "alphadesk_security": best.get("SECURITY") if best else None,
                "alphadesk_description": best.get("DESCRIPTION") if best else None,
                "alphadesk_symbol": best.get("COMPANY_SYMBOL") if best else None,
                "alphadesk_bloomberg_symbol": best.get("BBG_TICKER") if best else None,
                "alphadesk_sedol": best.get("SEDOL") if best else None,
                "matched_fields": (
                    best_candidate["matched_fields"] if best_candidate else []
                ),
                "conflicting_fields": (
                    best_candidate["conflicting_fields"] if best_candidate else []
                ),
                "candidate_count": len(candidates),
            }
        )
    return {
        "securities": results,
        "requested_count": len(results),
        "ready_count": sum(result["status"] == "ready" for result in results),
    }
