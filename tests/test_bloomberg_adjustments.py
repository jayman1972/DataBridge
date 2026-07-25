import unittest

from bloomberg.adjustments import (
    historical_adjustment_settings,
    validate_historical_adjustment_contract,
)


class BloombergAdjustmentProfileTests(unittest.TestCase):
    def test_maps_raw_capital_and_full_profiles(self) -> None:
        self.assertEqual(
            historical_adjustment_settings("raw"),
            {
                "adjustmentFollowDPDF": False,
                "adjustmentNormal": False,
                "adjustmentAbnormal": False,
                "adjustmentSplit": False,
            },
        )
        self.assertEqual(
            historical_adjustment_settings("capital"),
            {
                "adjustmentFollowDPDF": False,
                "adjustmentNormal": False,
                "adjustmentAbnormal": False,
                "adjustmentSplit": True,
            },
        )
        self.assertEqual(
            historical_adjustment_settings("full"),
            {
                "adjustmentFollowDPDF": False,
                "adjustmentNormal": True,
                "adjustmentAbnormal": True,
                "adjustmentSplit": True,
            },
        )

    def test_accepts_matching_explicit_contract(self) -> None:
        profile, error = validate_historical_adjustment_contract(
            {
                "adjustment_profile": "FULL",
                "adjustment_follow_dpdf": False,
                "adjustment_normal": True,
                "adjustment_abnormal": True,
                "adjustment_split": True,
            }
        )
        self.assertEqual(profile, "full")
        self.assertIsNone(error)

    def test_rejects_conflicting_or_unscoped_flags(self) -> None:
        _, conflict = validate_historical_adjustment_contract(
            {"adjustment_profile": "raw", "adjustment_split": True}
        )
        self.assertIn("conflicts", conflict or "")

        _, unscoped = validate_historical_adjustment_contract(
            {"adjustment_normal": True}
        )
        self.assertIn("adjustment_profile is required", unscoped or "")


if __name__ == "__main__":
    unittest.main()
