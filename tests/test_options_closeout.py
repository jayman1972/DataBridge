import unittest

from sggg.options_closeout import (
    evaluate_expiring_itm_positions,
    format_option_contract,
    parse_option_contract,
    select_live_underlying_quote,
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

    def test_prefers_same_day_last_price_over_prior_close(self):
        quote = select_live_underlying_quote(
            {
                "PX_LAST": 50.60,
                "PX_BID": 50.53,
                "PX_ASK": 50.55,
                "PX_MID": 50.54,
                "PX_OFFICIAL_CLOSE": 51.44,
                "LAST_UPDATE_DT": "2026-08-07",
            },
            expected_date="2026-08-07",
        )

        self.assertEqual(quote["price"], 50.60)
        self.assertEqual(quote["price_field"], "PX_LAST")

    def test_does_not_fall_back_to_prior_close_for_same_day_risk(self):
        quote = select_live_underlying_quote(
            {
                "PX_OFFICIAL_CLOSE": 51.44,
                "LAST_UPDATE_DT": "2026-08-06",
            },
            expected_date="2026-08-07",
        )

        self.assertIsNone(quote)

    def test_live_price_prevents_false_dram_call_flag(self):
        risks = evaluate_expiring_itm_positions(
            [{"security": "DRAM260807C00051000", "net_contracts": 1469}],
            "2026-08-07",
            {
                "DRAM": {
                    "price": 50.60,
                    "price_field": "PX_LAST",
                    "as_of_date": "2026-08-07",
                    "ticker": "DRAM US Equity",
                }
            },
        )

        self.assertEqual(risks, [])


if __name__ == "__main__":
    unittest.main()
