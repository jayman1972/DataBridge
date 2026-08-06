import unittest

from sggg.options_closeout import (
    evaluate_expiring_itm_positions,
    format_option_contract,
    parse_option_contract,
)


class OptionCloseoutExpiryRiskTest(unittest.TestCase):
    def test_parses_and_formats_occ_contract(self):
        parsed = parse_option_contract("SPY260805P00770000")
        self.assertEqual(parsed["underlying"], "SPY")
        self.assertEqual(parsed["expiration_date"], "2026-08-05")
        self.assertEqual(parsed["option_type"], "P")
        self.assertEqual(parsed["strike"], 770.0)
        self.assertEqual(format_option_contract("SPY260805P00770000"), "SPY 08/05/26 P770")

    def test_flags_long_itm_put_expiring_today(self):
        risks = evaluate_expiring_itm_positions(
            [{"security": "SPY260805P00770000", "net_contracts": 3}],
            "2026-08-05",
            {"SPY": {"price": 769, "ticker": "SPY US Equity"}},
        )
        self.assertEqual(len(risks), 1)
        self.assertIn("Long 3 contracts", risks[0]["flag_reason"])

    def test_flags_short_itm_call_expiring_today(self):
        risks = evaluate_expiring_itm_positions(
            [{"security": "SPY260805C00770000", "net_contracts": -2}],
            "2026-08-05",
            {"SPY": {"price": 771, "ticker": "SPY US Equity"}},
        )
        self.assertEqual(len(risks), 1)
        self.assertIn("Short 2 contracts", risks[0]["flag_reason"])

    def test_does_not_flag_at_the_money_or_other_expiry(self):
        positions = [
            {"security": "SPY260805P00770000", "net_contracts": 1},
            {"security": "SPY260806P00770000", "net_contracts": -1},
        ]
        risks = evaluate_expiring_itm_positions(
            positions,
            "2026-08-05",
            {"SPY": {"price": 770, "ticker": "SPY US Equity"}},
        )
        self.assertEqual(risks, [])

    def test_ignores_missing_underlying_quote(self):
        risks = evaluate_expiring_itm_positions(
            [{"security": "SPY260805P00770000", "net_contracts": 1}],
            "2026-08-05",
            {},
        )
        self.assertEqual(risks, [])


if __name__ == "__main__":
    unittest.main()
