"""Regression checks for SOAR risk calculations and source continuity."""

from __future__ import annotations

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.soar_risk_math import (
    growth_series,
    historical_var_95,
    merge_model_returns,
    missing_weekdays,
    previous_friday,
    weighted_scenario_return,
)


class SoarRiskMathTests(unittest.TestCase):
    def test_previous_friday_for_monday_and_friday(self) -> None:
        self.assertEqual(previous_friday(date(2026, 10, 26)), date(2026, 10, 23))
        self.assertEqual(previous_friday(date(2026, 10, 30)), date(2026, 10, 23))

    def test_historical_var_uses_empirical_fifth_percentile(self) -> None:
        as_of = date(2026, 10, 23)
        days = [
            as_of - timedelta(days=offset)
            for offset in range(365, -1, -1)
            if (as_of - timedelta(days=offset)).weekday() < 5
        ]
        returns = [(day, -0.02 if index < 15 else 0.002) for index, day in enumerate(days)]
        risk, count = historical_var_95(returns, as_of)
        self.assertEqual(count, len(days))
        self.assertAlmostEqual(risk, 0.02)

    def test_historical_var_rejects_weekly_or_gapped_series(self) -> None:
        as_of = date(2026, 10, 23)
        weekly = [(as_of - timedelta(days=7 * index), 0.01) for index in range(53)]
        with self.assertRaisesRegex(ValueError, "240 daily"):
            historical_var_95(weekly, as_of)
        days = [
            as_of - timedelta(days=offset)
            for offset in range(365, -1, -1)
            if (as_of - timedelta(days=offset)).weekday() < 5
        ]
        gapped = [(day, 0.001) for day in days if not date(2026, 4, 6) <= day <= date(2026, 4, 10)]
        with self.assertRaisesRegex(ValueError, "weekday gap"):
            historical_var_95(gapped, as_of)

    def test_merge_model_returns_preserves_user_supplied_bridge(self) -> None:
        legacy = {date(2026, 10, 16): 0.00777}
        gap = {
            date(2026, 10, 16): 0.00777,
            date(2026, 10, 19): -0.00143,
            date(2026, 10, 20): 0.00193,
            date(2026, 10, 21): 0.00110,
            date(2026, 10, 22): -0.00037,
            date(2026, 10, 23): 0.00451,
        }
        production = {date(2026, 10, 26): 0.0027938524573329904}
        merged = merge_model_returns(legacy, gap, production)
        self.assertEqual(len(merged), 7)
        self.assertAlmostEqual(merged[date(2026, 10, 23)], 0.00451)
        self.assertEqual(missing_weekdays(date(2026, 10, 23), date(2026, 10, 26)), 0)

    def test_model_overlap_conflict_is_not_silent(self) -> None:
        day = date(2026, 10, 16)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            merge_model_returns({day: 0.00777}, {day: 0.01}, {})

    def test_growth_series_requires_current_model_date(self) -> None:
        actual = {date(2026, 10, 22): 0.01, date(2026, 10, 23): -0.02}
        with self.assertRaisesRegex(ValueError, "do not reach"):
            growth_series(actual, {date(2026, 10, 22): 0.01}, date(2026, 10, 23))
        actual_growth, model_growth, slippage = growth_series(
            actual,
            {date(2026, 10, 22): 0.02, date(2026, 10, 23): 0.01},
            date(2026, 10, 23),
        )
        self.assertEqual(len(actual_growth), 2)
        self.assertEqual(len(model_growth), 2)
        self.assertEqual(len(slippage), 2)

    def test_scenario_uses_signed_current_weights(self) -> None:
        result = weighted_scenario_return(
            {"XLE": 0.5, "XLB": -0.25},
            {"XLE": (100, 80), "XLB": (50, 40)},
        )
        self.assertAlmostEqual(result, -0.05)


if __name__ == "__main__":
    unittest.main()
