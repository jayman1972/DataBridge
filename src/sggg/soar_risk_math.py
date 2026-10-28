"""Pure calculations for the SOAR weekly risk report.

Return inputs are decimal fractions (0.01 means 1%).  The historical VaR
series is the realized PSC portfolio P&L divided by prior NAV plus flows;
it is deliberately distinct from the Class B NAV-per-share performance series.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from datetime import date, timedelta
from itertools import pairwise


def parse_iso_date(value: object) -> date:
    """Parse a date without accepting ambiguous localized strings."""
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError("Date must be YYYY-MM-DD")
    try:
        if len(value) != 10:
            raise ValueError("Date must be YYYY-MM-DD")
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Date must be YYYY-MM-DD") from exc


def previous_friday(today: date) -> date:
    """Report on the last completed Friday, including Monday runs."""
    days = (today.weekday() - 4) % 7
    if days == 0:
        days = 7
    return today - timedelta(days=days)


def week_start(as_of: date) -> date:
    return as_of - timedelta(days=as_of.weekday())


def prior_month_end(as_of: date) -> date:
    return as_of.replace(day=1) - timedelta(days=1)


def prior_quarter_end(as_of: date) -> date:
    first_month = ((as_of.month - 1) // 3) * 3 + 1
    return as_of.replace(month=first_month, day=1) - timedelta(days=1)


def prior_year_end(as_of: date) -> date:
    return as_of.replace(month=1, day=1) - timedelta(days=1)


def missing_weekdays(start: date, end: date) -> int:
    """Count unobserved weekdays strictly between two observations."""
    return sum(
        (start + timedelta(days=offset)).weekday() < 5
        for offset in range(1, (end - start).days)
    )


def historical_var_95(
    daily_returns: Iterable[tuple[date, float]], as_of: date
) -> tuple[float, int]:
    """One-day 95% empirical loss quantile over a complete trailing year.

    A year must have at least 240 observations, start within seven calendar
    days of the trailing-year boundary, end on the report date, and have no
    gap of more than two missing weekdays. This prevents stale or weekly
    observations from masquerading as daily risk history.
    """
    start = as_of - timedelta(days=365)
    rows = sorted((day, float(ret)) for day, ret in daily_returns if start <= day <= as_of)
    if len(rows) < 240:
        raise ValueError(f"VaR requires at least 240 daily returns; found {len(rows)}")
    if rows[0][0] > start + timedelta(days=7) or rows[-1][0] != as_of:
        raise ValueError("VaR history does not cover the full trailing year")
    if len({day for day, _ in rows}) != len(rows):
        raise ValueError("VaR history has duplicate dates")
    if any(not math.isfinite(ret) or ret <= -1 for _, ret in rows):
        raise ValueError("VaR history has invalid daily returns")
    if any(missing_weekdays(left[0], right[0]) > 2 for left, right in pairwise(rows)):
        raise ValueError("VaR history has a material weekday gap")
    ordered = sorted(ret for _, ret in rows)
    position = 0.05 * (len(ordered) - 1)
    low = math.floor(position)
    high = math.ceil(position)
    fifth_percentile = ordered[low] + (ordered[high] - ordered[low]) * (position - low)
    return max(0.0, -fifth_percentile), len(rows)


def merge_model_returns(
    legacy: Mapping[date, float],
    gap: Mapping[date, float],
    production: Mapping[date, float],
) -> dict[date, float]:
    """Join source series, rejecting material overlaps rather than hiding them."""
    merged: dict[date, float] = {}
    for source in (legacy, gap, production):
        for day, raw_return in source.items():
            daily_return = float(raw_return)
            if not math.isfinite(daily_return) or daily_return <= -1:
                raise ValueError(f"Invalid model return on {day.isoformat()}")
            if day in merged and abs(merged[day] - daily_return) > 0.00001:
                raise ValueError(f"Conflicting model returns on {day.isoformat()}")
            merged[day] = daily_return
    return dict(sorted(merged.items()))


def growth_series(
    actual: Mapping[date, float], model: Mapping[date, float], as_of: date
) -> tuple[list[tuple[date, float]], list[tuple[date, float]], list[tuple[date, float]]]:
    """Build $100 growth curves and their comparable-date difference.

    Model observations on market holidays are absent, not synthetic zeroes.
    Consecutive missing weekdays in the model series are rejected if the
    model is otherwise expected to cover the report date.
    """
    actual_days = sorted(day for day in actual if day <= as_of)
    if not actual_days or actual_days[-1] != as_of:
        raise ValueError("Realized daily returns do not reach the report date")
    first_day = actual_days[0]
    model_days = sorted(day for day in model if first_day <= day <= as_of)
    if not model_days or model_days[-1] != as_of:
        raise ValueError("Production model returns do not reach the report date")
    if any(missing_weekdays(left, right) > 2 for left, right in pairwise(model_days)):
        raise ValueError("Model-return series has a material weekday gap")

    actual_growth: list[tuple[date, float]] = []
    model_growth: list[tuple[date, float]] = []
    actual_value = 100.0
    model_value = 100.0
    for day in sorted(set(actual_days) | set(model_days)):
        if day in actual:
            actual_value *= 1 + actual[day]
            actual_growth.append((day, actual_value))
        if day in model:
            model_value *= 1 + model[day]
            model_growth.append((day, model_value))
    actual_by_day = dict(actual_growth)
    model_by_day = dict(model_growth)
    slippage = [
        (day, actual_by_day[day] - model_by_day[day])
        for day in sorted(actual_by_day.keys() & model_by_day.keys())
    ]
    return actual_growth, model_growth, slippage


def weighted_scenario_return(
    position_weights: Mapping[str, float], price_pairs: Mapping[str, tuple[float, float]]
) -> float:
    """Apply each historical ETF price move to today's signed portfolio weight."""
    total = 0.0
    for ticker, weight in position_weights.items():
        start, end = price_pairs[ticker]
        if start <= 0 or end <= 0:
            raise ValueError(f"Invalid historical scenario price for {ticker}")
        total += weight * (end / start - 1)
    return total
