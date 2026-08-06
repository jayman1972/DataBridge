import unittest
from datetime import datetime

from sggg.options_closeout_alert import collect_flagged_groups, should_run_closeout_alert


class OptionsCloseoutAlertTest(unittest.TestCase):
    def test_runs_at_345_eastern_once_on_weekdays(self):
        now = datetime(2026, 8, 6, 15, 45)
        self.assertTrue(should_run_closeout_alert(now, None))
        self.assertFalse(should_run_closeout_alert(now, "2026-08-06"))

    def test_does_not_run_before_window_after_close_or_weekends(self):
        self.assertFalse(should_run_closeout_alert(datetime(2026, 8, 6, 15, 44), None))
        self.assertFalse(should_run_closeout_alert(datetime(2026, 8, 6, 16, 0), None))
        self.assertFalse(should_run_closeout_alert(datetime(2026, 8, 8, 15, 45), None))

    def test_collects_trade_and_expiry_reasons(self):
        report = {
            "trades": [
                {"security": "SPY260805P00770000", "flag_reason": "Wrong close action"},
            ],
            "groups": [
                {
                    "security": "SPY260805P00770000",
                    "security_display": "SPY 08/05/26 P770",
                    "net_end": 2,
                    "flags_count": 2,
                    "auto_exercise_risk": {"flag_reason": "Expires today ITM"},
                },
                {"security": "QQQ", "flags_count": 0},
            ],
        }
        flagged = collect_flagged_groups(report)
        self.assertEqual(len(flagged), 1)
        self.assertEqual(flagged[0]["reasons"], ["Wrong close action", "Expires today ITM"])


if __name__ == "__main__":
    unittest.main()
