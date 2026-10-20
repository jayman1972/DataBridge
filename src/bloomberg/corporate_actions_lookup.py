"""Explicit, bounded corporate-action lookups; never a universe backfill."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from typing import Any

from .blpapi_client import _coerce_blp_reference_value, blpapi

LOGGER = logging.getLogger(__name__)
ACTION_FIELDS = {
    "dividends": "DVD_HIST_ALL",
    "splits": "EQY_DVD_HIST_SPLITS",
    "mergers": "MERGERS_AND_ACQUISITIONS",
    "buybacks": "STOCK_BUYBACK_HISTORY",
    "offerings": "EQUITY_OFFERINGS",
    "rights": "DVD_HIST_ALL",
    "spinoffs": "DVD_HIST_ALL",
    "stock_dividends": "DVD_HIST_ALL",
}
SCOPES = {"security", "issuer", "security_plus"}
IDENTITY_FIELDS = ["PARSEKYABLE_DES", "SECURITY_NAME", "ID_BB_GLOBAL"]
ISSUER_FIELDS = [
    "ISSUER_PARENT_EQY_TICKER",
    "EQY_PRIM_SECURITY_TICKER",
    "EQY_PRIM_SECURITY_COMP_EXCH",
]


def normalize_identifier(value: str) -> str:
    text = re.sub(r"\s+", " ", value.strip())
    if not text or len(text) > 200 or any(ord(char) < 32 for char in text):
        raise ValueError(
            "Each security must be a non-empty identifier of at most 200 characters"
        )
    prefix = re.fullmatch(
        r"(CUSIP|SEDOL|ISIN)\s*[:= ]\s*([A-Z0-9]+)", text, re.IGNORECASE
    )
    if prefix:
        name, identifier = prefix.groups()
        return f"/{'sedol1' if name.upper() == 'SEDOL' else name.lower()}/{identifier.upper()}"
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", text, re.IGNORECASE):
        return f"/isin/{text.upper()}"
    if re.fullmatch(r"[A-Z0-9]{8}[0-9]", text, re.IGNORECASE):
        return f"/cusip/{text.upper()}"
    if re.fullmatch(r"[A-Z0-9]{6}[0-9]", text, re.IGNORECASE):
        return f"/sedol1/{text.upper()}"
    if re.fullmatch(r"[A-Z0-9./-]+\s+[A-Z]{2}", text, re.IGNORECASE):
        return text + " Equity"
    return text


def validate_request(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TypeError("A JSON object is required")
    raw = payload.get("securities")
    if (
        not isinstance(raw, list)
        or not 1 <= len(raw) <= 10
        or not all(isinstance(item, str) for item in raw)
    ):
        raise ValueError("Supply 1–10 security identifiers per batch")
    securities = list(dict.fromkeys(normalize_identifier(item) for item in raw))
    scope = payload.get("scope", "security_plus")
    basis = payload.get("date_basis", "effective")
    types = payload.get("action_types", list(ACTION_FIELDS))
    if scope not in SCOPES or basis not in {"announced", "effective"}:
        raise ValueError("Invalid scope or date basis")
    if (
        not isinstance(types, list)
        or not types
        or not all(isinstance(item, str) and item in ACTION_FIELDS for item in types)
    ):
        raise ValueError("Select supported action types")
    try:
        start, end = (
            date.fromisoformat(payload[name]) for name in ("start_date", "end_date")
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("start_date and end_date must be YYYY-MM-DD") from exc
    if end < start or (end - start).days > 800:
        raise ValueError("Date range must be ordered and no longer than 800 days")
    return {
        "securities": securities,
        "scope": scope,
        "date_basis": basis,
        "action_types": list(dict.fromkeys(types)),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }


def fetch_batch(
    client: Any,
    securities: list[str],
    fields: list[str],
    overrides: dict[str, str] | None = None,
    *,
    deadline: float | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Lossless ReferenceDataRequest with per-security errors and a hard deadline."""
    if not securities:
        return {}, []
    deadline = min(deadline or time.monotonic() + 30, time.monotonic() + 30)
    session = client._create_session()
    if not session:
        raise RuntimeError("Bloomberg Terminal is unavailable")
    result = {security: {} for security in securities}
    errors: list[str] = []
    try:
        req = session.getService(client.service).createRequest("ReferenceDataRequest")
        for security in dict.fromkeys(securities):
            req.getElement("securities").appendValue(security)
        for field in dict.fromkeys(fields):
            req.getElement("fields").appendValue(field)
        for name, value in (overrides or {}).items():
            override = req.getElement("overrides").appendElement()
            override.setElement("fieldId", name)
            override.setElement("value", value)
        session.sendRequest(req)
        complete = False
        while time.monotonic() < deadline:
            event = session.nextEvent(250)
            if event.eventType() in (
                blpapi.Event.RESPONSE,
                blpapi.Event.PARTIAL_RESPONSE,
            ):
                for message in event:
                    client._raise_for_response_error(message)
                    if not message.hasElement("securityData"):
                        continue
                    data = message.getElement("securityData")
                    for index in range(data.numValues()):
                        row = data.getValueAsElement(index)
                        sequence = (
                            row.getElementAsInteger("sequenceNumber")
                            if row.hasElement("sequenceNumber")
                            else index
                        )
                        security = (
                            securities[sequence]
                            if 0 <= sequence < len(securities)
                            else row.getElementAsString("security")
                        )
                        target = result.setdefault(security, {})
                        if row.hasElement("securityError"):
                            errors.append(
                                f"{security}: {row.getElement('securityError')}"
                            )
                            continue
                        values = row.getElement("fieldData")
                        for field in fields:
                            if not values.hasElement(field):
                                continue
                            element = values.getElement(field)
                            if element.isNull():
                                continue
                            if element.isArray():
                                target[field] = client._extract_bulk_field_rows(element)
                            else:
                                value = _coerce_blp_reference_value(element.getValue())
                                target[field] = (
                                    value.isoformat()
                                    if isinstance(value, (date, datetime))
                                    else value
                                )
                        if row.hasElement("fieldExceptions"):
                            exceptions = row.getElement("fieldExceptions")
                            for error_index in range(exceptions.numValues()):
                                error = exceptions.getValueAsElement(error_index)
                                errors.append(
                                    f"{security}/{error.getElementAsString('fieldId')}: {error.getElement('errorInfo')}"
                                )
            if event.eventType() == blpapi.Event.RESPONSE:
                complete = True
                break
        if not complete:
            errors.append("Bloomberg request timed out; results are incomplete")
        return result, errors
    finally:
        session.stop()


def row_value(row: dict[str, Any], *names: str) -> Any:
    normalized = {
        re.sub(r"[^a-z0-9]", "", key.lower()): value for key, value in row.items()
    }
    return next(
        (
            normalized[re.sub(r"[^a-z0-9]", "", name.lower())]
            for name in names
            if re.sub(r"[^a-z0-9]", "", name.lower()) in normalized
        ),
        None,
    )


def iso_date(value: Any) -> str | None:
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return None


def summarize(row: dict[str, Any]) -> str:
    return "; ".join(
        f"{key}: {json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value}"
        for key, value in row.items()
        if value is not None
    )


def distribution_type(row: dict[str, Any]) -> str:
    label = str(row_value(row, "Dividend Type") or "").lower()
    if "split" in label:
        return "splits"
    if "right" in label:
        return "rights"
    if "spin" in label:
        return "spinoffs"
    if "stock" in label:
        return "stock_dividends"
    return "dividends"


def action_id(row: dict[str, Any]) -> str | None:
    value = row_value(row, "Action Id")
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value) if value is not None and str(value).isdigit() else None


def lookup(client: Any, options: dict[str, Any]) -> dict[str, Any]:
    """Resolve scope, batch fields, enrich Action IDs, and filter on actual dates."""
    deadline = time.monotonic() + 105
    sources = options["securities"]
    warnings = [
        "API-derived lookup, not the complete CACT screen. Listings, delistings, name/ticker changes and other unsupported action families are not included.",
        "Only announced Bloomberg data is available; a future window is not a forecast. Related equities do not expand ETF/fund holdings.",
    ]
    errors: list[str] = []
    source_fields = IDENTITY_FIELDS + (
        ISSUER_FIELDS if options["scope"] != "security" else []
    )
    if options["scope"] == "security_plus":
        source_fields += ["RELATED_EQUITIES"]
    identity, identity_errors = fetch_batch(
        client, sources, source_fields, deadline=deadline
    )
    # Issuer-parent is not applicable to ordinary equities. Other mapping failures remain visible.
    errors += [
        error
        for error in identity_errors
        if not ("/ISSUER_PARENT_EQY_TICKER:" in error and "NOT_APPLICABLE" in error)
    ]
    universe: dict[str, set[str]] = {}
    mapping: list[dict[str, Any]] = []
    # Source securities always take priority over expansion when the bound is reached.
    for source, info in identity.items():
        if info.get("ID_BB_GLOBAL"):
            universe.setdefault(str(info.get("PARSEKYABLE_DES") or source), set()).add(
                source
            )
    for source in sources:
        info = identity.get(source, {})
        canonical = str(info.get("PARSEKYABLE_DES") or source)
        if not info.get("ID_BB_GLOBAL"):
            errors.append(f"{source}: Bloomberg identity could not be resolved")
            continue
        additions = [canonical]
        if options["scope"] != "security":
            parent = info.get("ISSUER_PARENT_EQY_TICKER")
            primary, exchange = (
                info.get("EQY_PRIM_SECURITY_TICKER"),
                info.get("EQY_PRIM_SECURITY_COMP_EXCH"),
            )
            if parent:
                additions.append(normalize_identifier(str(parent)))
            if primary and exchange:
                additions.append(normalize_identifier(f"{primary} {exchange}"))
        if options["scope"] == "security_plus":
            for row in info.get("RELATED_EQUITIES", []):
                related = row_value(row, "Ticker and Exchange Code")
                if related:
                    additions.append(normalize_identifier(str(related)))
        additions = list(dict.fromkeys(additions))
        accepted: list[str] = []
        for security in additions:
            if security not in universe and len(universe) >= 500:
                warnings.append(
                    "Scope expansion exceeded 500 securities in this batch; narrow the source list or use security-only scope. Coverage is incomplete."
                )
                continue
            universe.setdefault(security, set()).add(source)
            accepted.append(security)
        mapping.append(
            {
                "source": source,
                "security": canonical,
                "name": info.get("SECURITY_NAME"),
                "resolved_count": len(accepted),
                "requested_count": len(additions),
            }
        )
    fields = list(
        dict.fromkeys([ACTION_FIELDS[kind] for kind in options["action_types"]])
    )
    payload: dict[str, dict[str, Any]] = {}
    for offset in range(0, len(universe), 75):
        chunk = list(universe)[offset : offset + 75]
        if time.monotonic() >= deadline:
            errors.append(
                "Lookup deadline reached; some expanded securities were not queried"
            )
            break
        # Unbounded dividend history is needed for announcement-date filtering: ex-date may lie outside the selected window.
        overrides = (
            {
                "DVD_START_DT": options["start_date"].replace("-", ""),
                "DVD_END_DT": options["end_date"].replace("-", ""),
            }
            if options["date_basis"] == "effective" and "DVD_HIST_ALL" in fields
            else None
        )
        data, batch_errors = fetch_batch(
            client, chunk, fields, overrides, deadline=deadline
        )
        payload.update(data)
        errors.extend(batch_errors)
    merger_ids: set[str] = set()
    offering_ids: set[str] = set()
    for values in payload.values():
        merger_ids.update(
            f"{identifier} Action"
            for row in values.get("MERGERS_AND_ACQUISITIONS", [])
            if (identifier := action_id(row))
        )
        offering_ids.update(
            f"{identifier} Action"
            for row in values.get("EQUITY_OFFERINGS", [])
            if (identifier := action_id(row))
        )
    details: dict[str, dict[str, Any]] = {}
    if options["date_basis"] == "effective":
        for ids, detail_fields in [
            (
                merger_ids,
                [
                    "CA_MA_COMPLETE_DT",
                    "CA_MA_EXPECTED_COMPLETION_DATE",
                    "CA_MA_DEAL_STATUS",
                    "CA_MA_TARGET_TICKER",
                    "CA_MA_ACQUIRER_TICKER",
                ],
            ),
            (offering_ids, ["LISTING_DATE"]),
        ]:
            ordered = sorted(ids)
            if len(ordered) > 1000:
                warnings.append(
                    "More than 1,000 Action IDs require date enrichment; results are incomplete. Narrow the query."
                )
            for offset in range(0, min(len(ordered), 1000), 100):
                if time.monotonic() >= deadline:
                    errors.append(
                        "Action-date enrichment timed out; effective-date results are incomplete"
                    )
                    break
                values, detail_errors = fetch_batch(
                    client,
                    ordered[offset : offset + 100],
                    detail_fields,
                    deadline=deadline,
                )
                details.update(values)
                errors.extend(detail_errors)
    events: dict[str, dict[str, Any]] = {}
    missing_dates: dict[str, int] = {}
    for security, values in payload.items():
        for field in fields:
            rows = values.get(field, [])
            if len(rows) >= {
                "DVD_HIST_ALL": 275,
                "EQY_DVD_HIST_SPLITS": 200,
                "STOCK_BUYBACK_HISTORY": 175,
            }.get(field, 10**9):
                warnings.append(
                    f"{security}/{field}: Bloomberg record limit reached; older events may be missing."
                )
            for row in rows:
                kind = (
                    distribution_type(row)
                    if field in {"DVD_HIST_ALL", "EQY_DVD_HIST_SPLITS"}
                    else next(
                        key for key, value in ACTION_FIELDS.items() if value == field
                    )
                )
                if kind not in options["action_types"]:
                    continue
                identifier = action_id(row)
                extra = details.get(f"{identifier} Action", {}) if identifier else {}
                announced = iso_date(
                    row_value(row, "Declared Date", "Announcement Date", "Buyback Date")
                )
                effective = iso_date(row_value(row, "Ex-Date"))
                estimated = False
                if kind == "mergers":
                    status = str(
                        extra.get("CA_MA_DEAL_STATUS")
                        or row_value(row, "Deal Status")
                        or ""
                    ).lower()
                    effective = iso_date(extra.get("CA_MA_COMPLETE_DT"))
                    if not effective and status in {"pending", "proposed"}:
                        effective = iso_date(
                            extra.get("CA_MA_EXPECTED_COMPLETION_DATE")
                        )
                        estimated = effective is not None
                elif kind == "offerings":
                    effective = iso_date(extra.get("LISTING_DATE"))
                selected_date = (
                    announced if options["date_basis"] == "announced" else effective
                )
                if not selected_date:
                    missing_dates[kind] = missing_dates.get(kind, 0) + 1
                    continue
                if not options["start_date"] <= selected_date <= options["end_date"]:
                    continue
                key = (
                    f"{kind}:{identifier}"
                    if identifier
                    else f"{kind}:{security}:{json.dumps(row, sort_keys=True, default=str)}"
                )
                if key in events:
                    events[key]["sources"] = sorted(
                        set(events[key]["sources"]) | universe[security]
                    )
                    events[key]["matched_securities"] = sorted(
                        set(events[key]["matched_securities"]) | {security}
                    )
                    continue
                events[key] = {
                    "id": key,
                    "security": security,
                    "sources": sorted(universe[security]),
                    "matched_securities": [security],
                    "action_type": kind,
                    "action_id": identifier,
                    "announced_date": announced,
                    "effective_date": effective,
                    "date": selected_date,
                    "estimated": estimated,
                    "summary": summarize({**row, **extra}),
                    "details": {**row, **extra},
                    "field": field,
                }
    for kind, count in missing_dates.items():
        warnings.append(
            f"{kind}: {count} records lack a usable {options['date_basis']} date and were excluded; this is not evidence of no events."
        )
    LOGGER.info(
        "Corporate action lookup source=%d expanded=%d fields=%s range=%s..%s basis=%s events=%d errors=%d",
        len(sources),
        len(universe),
        fields,
        options["start_date"],
        options["end_date"],
        options["date_basis"],
        len(events),
        len(errors),
    )
    return {
        "events": sorted(
            events.values(), key=lambda row: (row["date"], row["security"])
        ),
        "mapping": mapping,
        "warnings": list(dict.fromkeys(warnings)),
        "errors": list(dict.fromkeys(errors)),
        "queried_count": len(payload),
        "expanded_count": len(universe),
        "request": options,
        "as_of": datetime.now().astimezone().isoformat(),
    }
