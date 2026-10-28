"""Weekly Friday/holiday NAV strike selection, independent of month-end sheets."""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.diamond_client import DiamondNavUnavailableError
from sggg.soar_risk_data import (
    DiamondValuationDateMismatch,
    _valuation_dates_after_handoff,
    weekly_position_date,
)


class SoarWeeklyDateTests(unittest.TestCase):
    def test_normal_friday_wins_over_midweek_month_end(self) -> None:
        friday = date(2026, 10, 2)
        with patch("sggg.soar_risk_data._class_b_nav", return_value=({}, {})) as nav:
            self.assertEqual(weekly_position_date(object(), friday), friday)
            self.assertEqual(nav.call_count, 1)

    def test_friday_holiday_uses_last_exact_weekly_strike(self) -> None:
        friday = date(2026, 4, 3)  # Good Friday; SOAR struck on Thursday.

        def nav(_client: object, day: date) -> tuple[dict, dict]:
            if day == friday:
                raise DiamondValuationDateMismatch("Diamond returned 2026-04-10")
            if day == date(2026, 4, 2):
                return {}, {}
            self.fail(f"Unexpected NAV lookup: {day}")

        with patch("sggg.soar_risk_data._class_b_nav", side_effect=nav):
            self.assertEqual(weekly_position_date(object(), friday), date(2026, 4, 2))

    def test_unfinalized_friday_waits_instead_of_using_midweek_strike(self) -> None:
        friday = date(2026, 10, 30)
        pending = DiamondNavUnavailableError("not finalized", end_date="2026-10-31")
        bbg = Mock()
        bbg.get_historical_data.return_value = [
            {"date": "2026-10-29", "PX_LAST": 1.0},
            {"date": "2026-10-30", "PX_LAST": 1.0},
        ]
        with patch("sggg.soar_risk_data._class_b_nav", side_effect=pending) as nav:
            with self.assertRaisesRegex(DiamondNavUnavailableError, "not finalized"):
                weekly_position_date(object(), friday, bbg)
            self.assertEqual(nav.call_count, 1)

    def test_unfinalized_holiday_friday_uses_thursday_strike(self) -> None:
        friday = date(2026, 4, 3)
        pending = DiamondNavUnavailableError("not finalized", end_date="2026-04-10")
        bbg = Mock()
        bbg.get_historical_data.return_value = [{"date": "2026-04-02", "PX_LAST": 1.0}]

        def nav(_client: object, day: date) -> tuple[dict, dict]:
            if day == friday:
                raise pending
            self.assertEqual(day, date(2026, 4, 2))
            return {}, {}

        with patch("sggg.soar_risk_data._class_b_nav", side_effect=nav):
            self.assertEqual(weekly_position_date(object(), friday, bbg), date(2026, 4, 2))

    def test_intervening_month_end_affects_return_chain_not_report_date(self) -> None:
        def nav(_client: object, day: date) -> tuple[dict, dict]:
            self.assertEqual(day, date(2026, 10, 31))
            return {}, {}

        with (
            patch("sggg.soar_risk_data.weekly_position_date", side_effect=lambda _c, d, _b: d),
            patch("sggg.soar_risk_data._class_b_nav", side_effect=nav),
        ):
            self.assertEqual(
                _valuation_dates_after_handoff(object(), object(), date(2026, 11, 6)),
                [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 6)],
            )

    def test_no_extra_month_end_strike_is_not_required(self) -> None:
        with (
            patch("sggg.soar_risk_data.weekly_position_date", side_effect=lambda _c, d, _b: d),
            patch(
                "sggg.soar_risk_data._class_b_nav",
                side_effect=DiamondValuationDateMismatch("Diamond returned 2026-10-30"),
            ),
        ):
            self.assertEqual(
                _valuation_dates_after_handoff(object(), object(), date(2026, 11, 6)),
                [date(2026, 10, 30), date(2026, 11, 6)],
            )


if __name__ == "__main__":
    unittest.main()
