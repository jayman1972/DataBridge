"""Bloomberg Action-security lifecycle refresh for public-target M&A deals."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional


MERGER_ACTION_FIELDS = [
    "CA_MA_CASH_TERMS",
    "CA_MA_STOCK_TERMS",
    "CA_MA_PAYMENT_TYP",
    "CA_MA_TARGET_TICKER",
    "CA_MA_ACQUIRER_TICKER",
    "CA_MA_ANNOUNCED_DATE",
    "CA_MA_EXPECTED_COMPLETION_DATE",
    "CA_MA_COMPLETE_DT",
    "CA_MA_DEAL_STATUS",
]

TERMINAL_COMPLETED_STATUSES = {
    "COMPLETE",
    "COMPLETED",
    "CLOSED",
}
TERMINAL_WITHDRAWN_STATUSES = {
    "ABANDONED",
    "CANCELLED",
    "CANCELED",
    "TERMINATED",
    "WITHDRAWN",
}
TERMINAL_STATUSES = TERMINAL_COMPLETED_STATUSES | TERMINAL_WITHDRAWN_STATUSES

BLOOMBERG_EQUITY_SYMBOL_RE = re.compile(
    r"^[A-Z0-9][A-Z0-9./-]*\s+[A-Z]{1,4}(?:\s+EQUITY)?$",
    re.IGNORECASE,
)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.upper() in {
        "#N/A",
        "N/A",
        "NA",
        "NAN",
        "NONE",
        "NULL",
    }


def _text(value: Any) -> Optional[str]:
    return None if _is_missing(value) else str(value).strip()


def _date_iso(value: Any) -> Optional[str]:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass
    for fmt in ("%Y%m%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _leading_number(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", ""))
    return float(match.group(0)) if match else None


def parse_stock_exchange_ratio(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    text = str(value).strip()
    upper = text.upper()
    if any(token in upper for token in ("USD", "CAD", "$", "MLN", "MM", "BLN")):
        return None
    if "AQR" not in upper and "SH" not in upper and "/" in upper:
        return None
    return _leading_number(text)


def parse_cash_consideration_per_share(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    text = str(value).strip()
    upper = text.upper()
    if any(token in upper for token in ("MLN", "MM", "BLN", "BILLION", "MILLION")):
        return None
    if "/" not in upper and "SH" not in upper:
        return None
    return _leading_number(text)


def _normalized_payment_type(value: Any) -> str:
    return re.sub(
        r"\s+",
        "_",
        str(value or "").strip().upper().replace("-", "_"),
    )


def _consideration_amount(value: Any) -> Optional[tuple[float, str, Optional[str]]]:
    """Return a comparable consideration amount, basis, and currency.

    Bloomberg Action terms can be fixed per-share amounts (``11.25/sh.``) or
    total legs (``USD 140 Mln``). Exchange ratios are deliberately excluded.
    """
    if _is_missing(value):
        return None
    text = str(value).strip()
    upper = text.upper().replace(",", "")
    if "AQR" in upper:
        return None
    number = _leading_number(upper)
    if number is None or number < 0:
        return None
    currency_match = re.search(r"\b(USD|CAD|EUR|GBP|AUD|JPY)\b", upper)
    currency = currency_match.group(1) if currency_match else None
    if re.search(r"\b(BLN|BILLION)\b", upper):
        return number * 1_000_000_000, "total", currency
    if re.search(r"\b(MLN|MM|MILLION)\b", upper):
        return number * 1_000_000, "total", currency
    if "/" in upper or "PER SHARE" in upper or "TGT SH" in upper:
        return number, "per_share", currency
    return None


def derive_stock_consideration_fraction(
    payment_type: Any,
    stock_terms: Any,
    cash_terms: Any,
    acquirer_price_at_announcement: Optional[float] = None,
) -> Optional[float]:
    """Derive the 0..1 stock-funded share without treating unknown as cash."""
    payment = _normalized_payment_type(payment_type)
    if "_OR_" in payment or "ELECT" in payment:
        return None
    if payment in {"CASH", "CASH_ONLY", "ALL_CASH"}:
        return 0.0
    if payment in {"STOCK", "STOCK_ONLY", "ALL_STOCK", "SHARES"}:
        return 1.0

    stock_amount = _consideration_amount(stock_terms)
    cash_amount = _consideration_amount(cash_terms)
    if stock_amount and cash_amount:
        stock_value, stock_basis, stock_currency = stock_amount
        cash_value, cash_basis, cash_currency = cash_amount
        currencies_match = (
            not stock_currency
            or not cash_currency
            or stock_currency == cash_currency
        )
        if stock_basis == cash_basis and currencies_match:
            total = stock_value + cash_value
            if total > 0:
                return max(0.0, min(1.0, stock_value / total))

    exchange_ratio = parse_stock_exchange_ratio(stock_terms)
    cash_per_share = parse_cash_consideration_per_share(cash_terms)
    if (
        exchange_ratio is not None
        and cash_per_share is not None
        and acquirer_price_at_announcement is not None
        and acquirer_price_at_announcement > 0
    ):
        stock_value_per_share = exchange_ratio * acquirer_price_at_announcement
        total_per_share = stock_value_per_share + cash_per_share
        if total_per_share > 0:
            return max(0.0, min(1.0, stock_value_per_share / total_per_share))
    return None


def payment_uses_stock(payment_type: Any, stock_terms: Any) -> Optional[bool]:
    if parse_stock_exchange_ratio(stock_terms) is not None:
        return True
    payment = str(payment_type or "").strip().upper().replace(" ", "_")
    if any(token in payment for token in ("STOCK", "SHARE")):
        return True
    if payment in {"CASH", "CASH_ONLY", "ALL_CASH"}:
        return False
    return None


def _number(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _latest_historical_value(
    bloomberg_client: Any,
    symbol: str,
    announced_date: str,
    field: str,
    *,
    currency: Optional[str] = None,
) -> Optional[float]:
    end_date = date.fromisoformat(announced_date)
    start_date = end_date - timedelta(days=10)
    records = bloomberg_client.get_historical_data(
        ticker=symbol,
        fields=[field],
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        periodicity="DAILY",
        adjustment_profile="RAW",
        currency=currency,
    )
    eligible = [
        record
        for record in records
        if _date_iso(record.get("date"))
        and _date_iso(record.get("date")) <= announced_date
        and _number(record.get(field)) is not None
    ]
    if not eligible:
        return None
    eligible.sort(key=lambda record: _date_iso(record.get("date")) or "")
    return _number(eligible[-1].get(field))


def _historical_equity_symbol(value: Any) -> Optional[str]:
    symbol = _text(value)
    if not symbol:
        return None
    if re.search(r"\s+EQUITY$", symbol, re.IGNORECASE):
        return re.sub(r"\s+EQUITY$", " Equity", symbol, flags=re.IGNORECASE)
    if re.search(r"\s+[A-Z]{1,4}$", symbol, re.IGNORECASE):
        return f"{symbol} Equity"
    return symbol


def enrich_stock_funding_metrics(
    row: Dict[str, Any],
    bloomberg_client: Any,
    history_cache: Dict[tuple[str, str, str, Optional[str]], Optional[float]],
    warnings: List[str],
) -> None:
    """Add announcement-date funding metrics needed by ModelBuilder."""
    fraction = _number(row.get("stock_consideration_fraction"))
    if fraction is None:
        fraction = derive_stock_consideration_fraction(
            row.get("payment_type"),
            row.get("stock_terms"),
            row.get("cash_terms"),
        )
    uses_stock = row.get("uses_stock_consideration") is True
    source_symbol = _text(row.get("acquirer_bloomberg_symbol"))
    symbol = _historical_equity_symbol(source_symbol)
    announced_date = _date_iso(row.get("announced_date"))
    valid_symbol = row.get("acquirer_ticker_is_valid") is True

    def history(field: str, currency: Optional[str] = None) -> Optional[float]:
        if not symbol or not announced_date:
            return None
        key = (symbol, announced_date, field, currency)
        if key not in history_cache:
            try:
                history_cache[key] = _latest_historical_value(
                    bloomberg_client,
                    symbol,
                    announced_date,
                    field,
                    currency=currency,
                )
            except Exception as exc:  # enrichment gaps must not break lifecycle updates
                history_cache[key] = None
                warnings.append(
                    f"{row.get('action_id')} {source_symbol or symbol} {field}: {exc}"
                )
        return history_cache[key]

    payment = _normalized_payment_type(row.get("payment_type"))
    terms_are_elective = "_OR_" in payment or "ELECT" in payment
    if fraction is None and uses_stock and valid_symbol and not terms_are_elective:
        local_price = history("PX_LAST")
        fraction = derive_stock_consideration_fraction(
            row.get("payment_type"),
            row.get("stock_terms"),
            row.get("cash_terms"),
            local_price,
        )
    if fraction is not None:
        row["stock_consideration_fraction"] = fraction

    existing_stock_value = _number(row.get("stock_consideration_value_usd"))
    market_cap = _number(row.get("acquirer_market_cap_at_announcement_usd"))
    needs_market_cap = uses_stock and (
        fraction is not None or existing_stock_value is not None
    )
    if market_cap is None and needs_market_cap and valid_symbol:
        market_cap_millions = history("CUR_MKT_CAP", "USD")
        if market_cap_millions is not None:
            market_cap = market_cap_millions * 1_000_000
            row["acquirer_market_cap_at_announcement_usd"] = market_cap

    deal_value = _number(row.get("deal_value_usd"))
    if (
        row.get("deal_value_to_acquirer_market_cap") is None
        and deal_value is not None
        and market_cap is not None
        and market_cap > 0
    ):
        row["deal_value_to_acquirer_market_cap"] = deal_value / market_cap

    stock_value = existing_stock_value
    if stock_value is None and fraction is not None and deal_value is not None:
        stock_value = fraction * deal_value
        row["stock_consideration_value_usd"] = stock_value
        row["stock_consideration_calculation_method"] = (
            "stock_fraction_x_deal_value"
        )
    if (
        row.get("stock_issuance_to_acquirer_market_cap") is None
        and stock_value is not None
        and market_cap is not None
        and market_cap > 0
    ):
        row["stock_issuance_to_acquirer_market_cap"] = stock_value / market_cap


def split_equity_symbols(value: Any) -> List[str]:
    if _is_missing(value):
        return []
    return [
        token.strip()
        for token in re.split(r"[,;\n|]+", str(value))
        if token.strip()
    ]


def ticker_classification(value: Any) -> tuple[int, bool]:
    symbols = split_equity_symbols(value)
    return (
        len(symbols),
        len(symbols) == 1
        and BLOOMBERG_EQUITY_SYMBOL_RE.match(symbols[0]) is not None,
    )


def normalize_status(value: Any) -> Optional[str]:
    text = _text(value)
    return text.upper().replace(" ", "_") if text else None


def should_monitor_deal(
    row: Dict[str, Any],
    as_of_date: date,
    terminal_recheck_days: int,
) -> bool:
    status = normalize_status(row.get("status"))
    if status not in TERMINAL_STATUSES:
        return True
    terminal_date = _date_iso(
        row.get("actual_completion_date") or row.get("withdrawal_date")
    )
    if terminal_date is None:
        return True
    return date.fromisoformat(terminal_date) >= (
        as_of_date - timedelta(days=max(0, terminal_recheck_days))
    )


def build_ingest_row(
    existing: Dict[str, Any],
    reference: Dict[str, Any],
) -> Dict[str, Any]:
    status = normalize_status(
        reference.get("CA_MA_DEAL_STATUS") or existing.get("status")
    )
    complete_date = _date_iso(reference.get("CA_MA_COMPLETE_DT"))
    payment_type = _text(reference.get("CA_MA_PAYMENT_TYP"))
    stock_terms = _text(reference.get("CA_MA_STOCK_TERMS"))
    cash_terms = _text(reference.get("CA_MA_CASH_TERMS"))
    acquirer_symbol = (
        _text(reference.get("CA_MA_ACQUIRER_TICKER"))
        or _text(existing.get("acquirer_bloomberg_symbol"))
    )
    target_symbol = (
        _text(reference.get("CA_MA_TARGET_TICKER"))
        or _text(existing.get("target_bloomberg_symbol"))
    )
    ticker_count, ticker_is_valid = ticker_classification(acquirer_symbol)

    row: Dict[str, Any] = {
        "action_id": str(existing["action_id"]).strip(),
        "announced_date": (
            _date_iso(reference.get("CA_MA_ANNOUNCED_DATE"))
            or _date_iso(existing.get("announced_date"))
        ),
        "expected_completion_date": _date_iso(
            reference.get("CA_MA_EXPECTED_COMPLETION_DATE")
        ),
        "status": status,
        "payment_type": payment_type,
        "cash_terms": cash_terms,
        "stock_terms": stock_terms,
        "acquirer_bloomberg_symbol": acquirer_symbol,
        "target_bloomberg_symbol": target_symbol,
        "target_is_private": existing.get("target_is_private"),
        "acquirer_ticker_count": ticker_count,
        "acquirer_ticker_is_valid": ticker_is_valid,
        "eligible_for_model_builder": existing.get("target_is_private") is False,
        "uses_stock_consideration": payment_uses_stock(payment_type, stock_terms),
        "stock_exchange_ratio": parse_stock_exchange_ratio(stock_terms),
        "cash_consideration_per_share":
            parse_cash_consideration_per_share(cash_terms),
        # The Action reference fields above do not expose a reliable stock
        # funding percentage or announcement-date market cap. Preserve values
        # populated by the BQL/backfill workflow on every lifecycle refresh.
        "deal_value_usd": existing.get("deal_value_usd"),
        "acquirer_market_cap_at_announcement_usd": existing.get(
            "acquirer_market_cap_at_announcement_usd"
        ),
        "deal_value_to_acquirer_market_cap": existing.get(
            "deal_value_to_acquirer_market_cap"
        ),
        "stock_consideration_fraction": existing.get(
            "stock_consideration_fraction"
        ),
        "stock_consideration_value_usd": existing.get(
            "stock_consideration_value_usd"
        ),
        "stock_issuance_to_acquirer_market_cap": existing.get(
            "stock_issuance_to_acquirer_market_cap"
        ),
        "stock_consideration_calculation_method": existing.get(
            "stock_consideration_calculation_method"
        ),
    }
    if status in TERMINAL_COMPLETED_STATUSES:
        row["actual_completion_date"] = complete_date
    elif status in TERMINAL_WITHDRAWN_STATUSES:
        row["withdrawal_date"] = complete_date

    return {key: value for key, value in row.items() if value is not None}


def _response_data(response: Any) -> List[Dict[str, Any]]:
    if response is None:
        return []
    data = getattr(response, "data", None)
    if data is None and isinstance(response, dict):
        data = response.get("data")
    if isinstance(data, dict):
        return [data]
    return list(data or [])


def load_merger_deals(supabase: Any) -> List[Dict[str, Any]]:
    selected = (
        "action_id,announced_date,status,actual_completion_date,withdrawal_date,"
        "acquirer_bloomberg_symbol,target_bloomberg_symbol,target_is_private,"
        "deal_value_usd,acquirer_market_cap_at_announcement_usd,"
        "deal_value_to_acquirer_market_cap,stock_consideration_fraction,"
        "stock_consideration_value_usd,"
        "stock_issuance_to_acquirer_market_cap,"
        "stock_consideration_calculation_method"
    )
    rows: List[Dict[str, Any]] = []
    page_size = 1000
    offset = 0
    while True:
        response = (
            supabase.table("security_merger_deals")
            .select(selected)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = _response_data(response)
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def _chunks(values: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def refresh_open_merger_actions(
    supabase: Any,
    bloomberg_client: Any,
    as_of_date: date,
    *,
    batch_size: int = 50,
    terminal_recheck_days: int = 5,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if supabase is None:
        raise RuntimeError("Supabase client is not configured")
    if bloomberg_client is None:
        raise RuntimeError("Bloomberg client is not configured")

    all_deals = load_merger_deals(supabase)
    candidates = [
        row
        for row in all_deals
        if row.get("target_is_private") is not True
        and should_monitor_deal(row, as_of_date, terminal_recheck_days)
    ]
    errors: List[str] = []
    warnings: List[str] = []
    ingest_rows: List[Dict[str, Any]] = []
    history_cache: Dict[
        tuple[str, str, str, Optional[str]], Optional[float]
    ] = {}
    for batch in _chunks(candidates, max(1, min(int(batch_size), 100))):
        action_security_by_id = {
            str(row["action_id"]).strip(): f"{str(row['action_id']).strip()} Action"
            for row in batch
        }
        references = bloomberg_client.get_reference_data(
            list(action_security_by_id.values()),
            MERGER_ACTION_FIELDS,
        )
        for existing in batch:
            action_id = str(existing["action_id"]).strip()
            security = action_security_by_id[action_id]
            reference = references.get(security, {})
            if reference.get("error"):
                errors.append(f"{security}: {reference['error']}")
                continue
            row = build_ingest_row(existing, reference)
            if not row.get("announced_date"):
                errors.append(f"{security}: missing announced date")
                continue
            enrich_stock_funding_metrics(
                row,
                bloomberg_client,
                history_cache,
                warnings,
            )
            ingest_rows.append(row)

    rpc_results: List[Any] = []
    if not dry_run:
        for batch in _chunks(ingest_rows, max(1, min(int(batch_size), 100))):
            response = supabase.rpc(
                "ingest_security_merger_deals",
                {
                    "p_as_of_date": as_of_date.isoformat(),
                    "p_rows": batch,
                    "p_source": "bloomberg",
                },
            ).execute()
            rpc_results.extend(_response_data(response))

    stock_true = sum(row.get("uses_stock_consideration") is True for row in ingest_rows)
    stock_false = sum(row.get("uses_stock_consideration") is False for row in ingest_rows)
    stock_issuance_ratio_known = sum(
        row.get("stock_issuance_to_acquirer_market_cap") is not None
        for row in ingest_rows
    )
    return {
        "success": len(errors) == 0,
        "dry_run": dry_run,
        "as_of_date": as_of_date.isoformat(),
        "deals_in_database": len(all_deals),
        "actions_requested": len(candidates),
        "actions_refreshed": len(ingest_rows),
        "stock_consideration_true": stock_true,
        "stock_consideration_false": stock_false,
        "stock_consideration_unknown": len(ingest_rows) - stock_true - stock_false,
        "stock_issuance_ratio_known": stock_issuance_ratio_known,
        "errors": errors,
        "warnings": warnings,
        "rpc_results": rpc_results,
        "preview": ingest_rows[:10] if dry_run else [],
    }
