"""Regression coverage for matching PSC holdings and EMSX fills by Bloomberg ticker."""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from sggg.options_closeout import canonical_futures_option_key, parse_option_contract
from sggg.options_closeout_coverage import finish_coverage, load_start_positions
from test_options_closeout_coverage import Cursor, legacy_helpers


def emsx_fills(rows):
    """Execute the real GetFills parser against an offline Bloomberg response."""
    helpers = legacy_helpers()
    blpapi = MagicMock()
    message = MagicMock()
    message.correlationIds.return_value = []
    message.messageType.return_value = "GetFillsResponse"
    message.hasElement.side_effect = lambda name: name == "Fills"
    array = message.getElement.return_value
    array.numValues.return_value = len(rows)
    array.getValueAsElement.side_effect = [SimpleNamespace(
        hasElement=lambda name, row=row: name in row,
        getElementAsString=lambda name, row=row: str(row[name]),
        getElementAsFloat=lambda name, row=row: float(row[name]),
    ) for row in rows]
    event = MagicMock()
    event.eventType.return_value = blpapi.Event.RESPONSE
    event.__iter__.return_value = iter([message])
    blpapi.Session.return_value.nextEvent.return_value = event
    helpers["_try_import_blpapi"] = lambda: (blpapi, None)
    return helpers["_emsx_history_get_fills"]("2026-11-24", team="PM")


class BloombergCloseoutIdentityTest(unittest.TestCase):
    def test_normalizes_futures_option_ticker_without_guessing_expiry(self):
        key = canonical_futures_option_key("  4Ox6P    25500.000 Index ")
        self.assertEqual(key, "4OX6P 25500 INDEX")
        self.assertIsNone(parse_option_contract(key))
        self.assertEqual(canonical_futures_option_key("CLU6C 85 Comdty"), "CLU6C 85 COMDTY")
        self.assertEqual(canonical_futures_option_key("QNBWX6P4 P26500 Index"), "QNBWX6P4 26500 INDEX")

    def test_rejects_plain_futures_stocks_and_conflicting_put_call_codes(self):
        for value in ("NQZ6 Index", "GLD US Equity", "NSD100 EMIN WK4 O NOV26P 25500", "QNBWX6P4 C26500 Index"):
            with self.subTest(value=value):
                self.assertIsNone(canonical_futures_option_key(value))

    def test_futures_option_fill_is_retained_with_blank_name_and_occ(self):
        fills = emsx_fills([
            {"Ticker": "4OX6P    25500", "YellowKey": "Index", "AssetClass": "Option", "Side": "S", "FillShares": 1, "FillPrice": 106, "OrderId": "25435989"},
            {"Ticker": "NQZ6", "YellowKey": "Index", "AssetClass": "Future", "Side": "S", "FillShares": 2},
            {"Ticker": "GLD", "YellowKey": "Equity", "AssetClass": "Equity", "Side": "B", "FillShares": 100},
        ])
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]["SECURITY"], "4OX6P 25500 INDEX")
        self.assertEqual(fills[0]["SECURITY_DISPLAY"], "4OX6P 25500 INDEX")
        self.assertEqual(fills[0]["ACT_QTTY"], 1)
        self.assertEqual(fills[0]["PRICE"], 106)
        self.assertEqual(fills[0]["FILTER_DEBUG"]["filtered_out_count"], 2)

    def test_bloomberg_equity_option_ticker_is_preferred_to_display_name(self):
        fill = emsx_fills([{
            "Ticker": "SPY US 11/24/26 P712", "YellowKey": "Equity", "AssetClass": "Option",
            "SecurityName": "SPY 11/24/26 P713", "Side": "S", "FillShares": 1,
        }])[0]
        self.assertEqual(fill["SECURITY"], "SPY261124P00712000")

    def test_psc_bloomberg_ticker_and_emsx_sell_offset_to_flat(self):
        helpers = legacy_helpers()
        raw = "NSD100 EMIN WK4 O NOV26P 25500"
        starting, coverage = load_start_positions(
            Cursor({"Growth": [(raw, "SHORT", 1, "4OX6P 25500 Index")]}),
            ["Growth"], "2026-11-24", lambda *a, **kw: "20261123",
            helpers["_canonical_option_key"], helpers["_normalize_option_desc_key"],
        )
        self.assertEqual(starting, {"4OX6P 25500 INDEX": 1})
        fill = emsx_fills([{"Ticker": "4OX6P    25500", "YellowKey": "Index", "Side": "S", "FillShares": 1}])
        analysis = helpers["_options_closeout_analyze"](fill, starting)
        finish_coverage(coverage, analysis, parse_option_contract, "complete")
        self.assertEqual(len(analysis["groups"]), 1)
        group = analysis["groups"][0]
        self.assertEqual(group["net_end"], 0)
        self.assertEqual(group["trades_count"], 1)
        self.assertEqual(group["flags_count"], 0)
        self.assertEqual(analysis["trades"][0]["net_before"], 1)
        self.assertEqual(analysis["trades"][0]["net_after"], 0)

    def test_different_strikes_and_weekly_contracts_cannot_cancel(self):
        helpers = legacy_helpers()
        fill = emsx_fills([{"Ticker": "4OX6P 25700", "YellowKey": "Index", "Side": "S", "FillShares": 1}])
        analysis = helpers["_options_closeout_analyze"](fill, {"4OX6P 25500 INDEX": 1, "QNBWX6P4 25700 INDEX": 1})
        finish_coverage({"issues": []}, analysis, parse_option_contract, "complete")
        groups = {g["security"]: g for g in analysis["groups"]}
        self.assertEqual(len(groups), 3)
        self.assertEqual(groups["4OX6P 25500 INDEX"]["net_end"], 1)
        self.assertTrue(all(g["flags_count"] == 1 for g in groups.values()))

    def test_actual_endpoint_matches_bbg_ticker_before_opaque_psc_description(self):
        from test_options_closeout_coverage import CloseoutCoverageTest

        ns = CloseoutCoverageTest().endpoint_namespace()
        cursor = Cursor({"Growth": [("NSD100 EMIN WK4 O NOV26P 25500", "SHORT", 1, "4OX6P 25500 Index")]})
        conn = SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
        ns["_pyodbc_or_503"] = lambda: (SimpleNamespace(connect=lambda *a, **kw: conn), None)
        ns["_emsx_history_get_fills"] = lambda **kw: emsx_fills([{"Ticker": "4OX6P 25500", "YellowKey": "Index", "Side": "S", "FillShares": 1}])
        result, status = ns["emsx_options_closeout_check"]()
        self.assertEqual(status, 200)
        self.assertEqual(result["groups"][0]["net_end"], 0)
        self.assertEqual(result["groups"][0]["flags_count"], 0)
        self.assertEqual(result["groups"][0]["trades_count"], 1)


if __name__ == "__main__":
    unittest.main()
