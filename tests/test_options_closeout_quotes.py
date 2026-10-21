"""Exercise the report's quote-fetch path without starting DataBridge."""

import ast
import copy
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List
import unittest
from unittest.mock import Mock

from sggg.options_closeout import evaluate_expiring_itm_positions


REPORT_DATE = "2026-10-21"


def load_report_quote_fetcher(client):
    # Importing the Flask application would initialize live integrations and
    # read host configuration. Execute only the real quote function and its
    # pure-helper imports, with a fake Bloomberg client and a fixed date.
    source_path = Path(__file__).resolve().parents[1] / "data_bridge.py"
    source = ast.parse(source_path.read_text(encoding="utf-8"))
    imports = [
        node for node in source.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "sggg.options_closeout"
    ]
    function = next(
        node for node in source.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_option_underlying_quotes"
    )
    module = ast.Module(body=[*imports, function], type_ignores=[])
    namespace = {
        "Dict": Dict,
        "List": List,
        "bloomberg_client": client,
        "_eastern_today_iso": lambda: REPORT_DATE,
    }
    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace["_option_underlying_quotes"]


class OptionCloseoutQuoteMappingTest(unittest.TestCase):
    def quote_fetcher(self, response):
        client = SimpleNamespace(get_reference_data=Mock(return_value=response))
        return load_report_quote_fetcher(client), client.get_reference_data

    def test_vix_calls_and_puts_use_one_index_quote_without_rewriting_positions(self):
        positions = [
            {"security": "VIX261021C00020000", "net_contracts": 2},
            {"security": "VIX261021P00022000", "net_contracts": -1},
            {"security": "vix261028p00025000", "net_contracts": 3},
            {"security": "SPY261021P00700000", "net_contracts": 1},
        ]
        original = copy.deepcopy(positions)
        fetch, request = self.quote_fetcher({
            "VIX Index": {"PX_LAST": 21, "LAST_UPDATE_DT": REPORT_DATE},
            "SPY US Equity": {"PX_LAST": 710, "LAST_UPDATE_DT": REPORT_DATE},
        })

        quotes = fetch(positions)
        tickers = request.call_args.kwargs["tickers"]
        self.assertEqual(tickers, ["SPY US Equity", "SPY CN Equity", "VIX Index"])
        self.assertEqual(quotes["VIX"]["ticker"], "VIX Index")
        self.assertEqual(quotes["VIX"]["price"], 21)
        risks = evaluate_expiring_itm_positions(positions, REPORT_DATE, quotes)
        self.assertEqual({risk["security"] for risk in risks}, {
            "VIX261021C00020000", "VIX261021P00022000",
        })
        self.assertTrue(all(risk["underlying"] == "VIX" for risk in risks))
        self.assertTrue(all(risk["underlying_bloomberg_ticker"] == "VIX Index" for risk in risks))
        self.assertEqual(positions, original)

    def test_stale_index_does_not_fall_back_to_an_incorrect_vix_equity_quote(self):
        fetch, request = self.quote_fetcher({
            "VIX Index": {"PX_LAST": 21, "LAST_UPDATE_DT": "2026-10-20"},
            "VIX US Equity": {"PX_LAST": 999, "LAST_UPDATE_DT": REPORT_DATE},
            "VIX CN Equity": {"PX_LAST": 888, "LAST_UPDATE_DT": REPORT_DATE},
        })

        quotes = fetch([{"security": "VIX261021C00020000", "net_contracts": 1}])

        self.assertEqual(request.call_args.kwargs["tickers"], ["VIX Index"])
        self.assertNotIn("VIX", quotes)

    def test_non_vix_contracts_keep_equity_candidates_and_canadian_fallback(self):
        fetch, request = self.quote_fetcher({
            "XIU US Equity": {"error": "not found"},
            "XIU CN Equity": {"PX_LAST": 35, "LAST_UPDATE_DT": REPORT_DATE},
            "VIXY US Equity": {"PX_LAST": 15, "LAST_UPDATE_DT": REPORT_DATE},
        })

        quotes = fetch([
            {"security": "XIU261021C00030000", "net_contracts": 1},
            {"security": "VIXY261021C00010000", "net_contracts": 1},
        ])

        self.assertEqual(request.call_args.kwargs["tickers"], [
            "VIXY US Equity", "VIXY CN Equity", "XIU US Equity", "XIU CN Equity",
        ])
        self.assertEqual(quotes["XIU"]["ticker"], "XIU CN Equity")
        self.assertEqual(quotes["VIXY"]["ticker"], "VIXY US Equity")

    def test_vix_at_the_money_remains_unflagged(self):
        fetch, _ = self.quote_fetcher({
            "VIX Index": {"PX_LAST": 20, "LAST_UPDATE_DT": REPORT_DATE},
        })
        positions = [
            {"security": "VIX261021C00020000", "net_contracts": 1},
            {"security": "VIX261021P00020000", "net_contracts": -1},
        ]

        quotes = fetch(positions)

        self.assertEqual(evaluate_expiring_itm_positions(positions, REPORT_DATE, quotes), [])


if __name__ == "__main__":
    unittest.main()
