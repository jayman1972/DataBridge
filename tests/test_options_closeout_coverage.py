import ast
import re
import time
import unittest
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from datetime import datetime
from sggg.options_closeout import format_option_contract, parse_option_contract, select_live_underlying_quote
from sggg.options_closeout_coverage import finish_coverage, load_start_positions


def legacy_helpers():
    # Compile only pure analyzer definitions. Importing data_bridge would start
    # vendor clients and read production configuration, which tests must avoid.
    tree = ast.parse((Path(__file__).resolve().parents[1] / "data_bridge.py").read_text(encoding="utf-8"))
    names = {"_canonical_option_key", "_normalize_option_desc_key", "_options_closeout_analyze", "_emsx_history_get_fills", "emsx_options_closeout_check"}
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in names:
            n.decorator_list = []
    module = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
    namespace = {"re": re, "time": time, "List": List, "Dict": Dict, "Optional": Optional,
                 "format_option_contract": format_option_contract, "datetime": datetime}
    exec(compile(module, "isolated-closeout-helpers", "exec"), namespace)
    return namespace


class Cursor:
    __slots__ = ("rows", "portfolio")
    def __init__(self, rows):
        self.rows = rows
        self.portfolio = None

    def execute(self, _sql, args):
        self.portfolio = args[0]

    def fetchall(self):
        value = self.rows.get(self.portfolio, [])
        if isinstance(value, Exception):
            raise value
        return value


class CloseoutCoverageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helpers = legacy_helpers()

    def load(self, rows, snapshots=None):
        snapshots = snapshots or {p: "20261013" for p in ["Alpha", "Select", "Income", "Growth"]}
        return load_start_positions(Cursor(rows), ["Alpha", "Select", "Income", "Growth"], "2026-10-14",
                                    lambda _c, p, _d, **_k: snapshots.get(p),
                                    self.helpers["_canonical_option_key"], self.helpers["_normalize_option_desc_key"])

    def test_confirms_all_four_portfolios_including_explicit_zero_option_book(self):
        starting, coverage = self.load({"Alpha": [("SPY 10/14/26 C100", "L", 2)]})
        self.assertEqual(starting, {"SPY261014C00100000": 2})
        self.assertEqual(coverage["status"], "complete")
        self.assertEqual(len(coverage["portfolios"]), 4)
        self.assertEqual(coverage["portfolios"][1]["option_lines"], 0)

    def test_missing_snapshot_is_not_a_zero_book(self):
        _, coverage = self.load({}, {"Alpha": "20261013"})
        self.assertEqual(coverage["status"], "incomplete")
        self.assertEqual(sum(p["status"] == "incomplete" for p in coverage["portfolios"]), 3)

    def test_stale_and_future_snapshots_are_explicit(self):
        _, coverage = self.load({}, {"Alpha": "20261001", "Select": "20261014", "Income": "20261013", "Growth": "20261013"})
        self.assertEqual(coverage["status"], "incomplete")
        self.assertEqual(coverage["portfolios"][0]["snapshot_date"], "2026-10-01")

    def test_recent_but_missing_previous_session_is_incomplete_and_preserves_known_holdings(self):
        starting, coverage = load_start_positions(Cursor({"Alpha": [("SPY 10/14/26 C100", "L", 2)]}), ["Alpha"], "2026-10-14",
                                                lambda *a, **kw: "20261012", self.helpers["_canonical_option_key"], self.helpers["_normalize_option_desc_key"], "2026-10-13")
        self.assertEqual(starting["SPY261014C00100000"], 2)
        self.assertEqual(coverage["status"], "incomplete")
        self.assertEqual(coverage["portfolios"][0]["status"], "incomplete")
        self.assertIn("prior US session", "; ".join(coverage["issues"]))

    def test_psc_error_keeps_other_funds_known_positions_and_marks_incomplete(self):
        start, coverage = self.load({"Alpha": RuntimeError("PSC unavailable"), "Select": [("QQQ 10/14/26 P729", "S", -250)]})
        self.assertEqual(start["QQQ261014P00729000"], -250)
        self.assertIn("PSC unavailable", "; ".join(coverage["issues"]))

    def test_unknown_contract_and_nonfinite_quantity_do_not_pass(self):
        _, coverage = self.load({"Alpha": [("UNKNOWN OPTION", "L", 2)], "Select": [("SPY 10/14/26 C100", "L", float("inf"))]})
        self.assertEqual(coverage["status"], "incomplete")

    def test_opposite_funds_cannot_silently_cancel_risk_coverage(self):
        start, coverage = self.load({"Alpha": [("SPY 10/14/26 C100", "L", 2)], "Select": [("SPY 10/14/26 C100", "S", -2)]})
        self.assertEqual(start["SPY261014C00100000"], 0)
        self.assertEqual(coverage["status"], "incomplete")
        self.assertEqual(len(coverage["opposing_portfolio_positions"]), 1)

    def test_quote_failure_and_unknown_open_contract_are_not_complete(self):
        c = {"status": "complete", "issues": [], "portfolios": []}
        c = finish_coverage(c, {"open_positions": [{"security": "unparsed"}]}, parse_option_contract, "missing_quotes: SPY")
        self.assertEqual(c["status"], "incomplete")
        self.assertEqual(len(c["issues"]), 2)

    def test_quote_requires_same_day_timestamp_and_finite_price(self):
        for quote in [{"PX_LAST": 100}, {"PX_LAST": 100, "LAST_UPDATE_DT": "2026-10-13"}, {"PX_LAST": float("inf"), "LAST_UPDATE_DT": "2026-10-14"}]:
            self.assertIsNone(select_live_underlying_quote(quote, expected_date="2026-10-14"))

    def test_analyzer_retains_held_positions_without_trades(self):
        analyze = self.helpers["_options_closeout_analyze"]
        report = analyze([], {"SPY261014C00100000": 2})
        self.assertEqual(report["groups"][0]["trades_count"], 0)
        self.assertEqual(report["groups"][0]["net_end"], 2)

    def test_actual_analyzer_preserves_ibit_and_qqq_zero_end_action_flags(self):
        analyze = self.helpers["_options_closeout_analyze"]
        fills = [
            {"SECURITY": "IBIT261014P00046000", "ORDER_ID": "1", "ORDER_ACTION": "B", "ACT_QTTY": 500, "TRADE_DATE_TIME": "11:38:22"},
            {"SECURITY": "IBIT261014P00046000", "ORDER_ID": "2", "ORDER_ACTION": "BS", "ACT_QTTY": 500, "TRADE_DATE_TIME": "15:48:42"},
            {"SECURITY": "QQQ261014P00729000", "ORDER_ID": "3", "ORDER_ACTION": "B", "ACT_QTTY": 250, "TRADE_DATE_TIME": "13:21:50"},
        ]
        result = analyze(fills, {"IBIT261014P00046000": -500, "QQQ261014P00729000": -250})
        groups = {g["security"]: g for g in result["groups"]}
        self.assertEqual(groups["IBIT261014P00046000"]["net_end"], 500)
        self.assertEqual(groups["QQQ261014P00729000"]["net_end"], 0)
        self.assertEqual(sum(g["flags_count"] for g in groups.values()), 2)

    def test_emsx_times_out_before_missing_final_response(self):
        blpapi = MagicMock()
        self.helpers["_try_import_blpapi"] = lambda: (blpapi, None)
        with patch.object(time, "monotonic", side_effect=[0, 61]):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                self.helpers["_emsx_history_get_fills"]("2026-10-14", team="PM")
        blpapi.Session.return_value.stop.assert_called_once()

    def endpoint_namespace(self):
        ns = legacy_helpers()
        ns.update({"request": SimpleNamespace(method="POST", get_json=lambda **kw: {"date": "2026-10-14", "team": "PM", "use_psc_start_positions": True, "require_complete_coverage": True, "required_start_positions_date": "2026-10-13"}),
                   "_compact_to_ymd": lambda d: d, "_psc_portfolio_from_request": lambda d: None,
                   "PSC_ALL_FUNDS_PORTFOLIOS": ["Alpha", "Select", "Income", "Growth"],
                   "_PSC_START_POS_CACHE": {}, "_PSC_START_POS_CACHE_META": {},
                   "load_start_positions": load_start_positions, "finish_coverage": finish_coverage,
                   "parse_option_contract": parse_option_contract,
                   "evaluate_expiring_itm_positions": lambda *args: [],
                   "_eastern_today_iso": lambda: "2026-10-14", "_log_serialize": lambda x: x,
                   "_json_response": lambda x, status: (x, status), "jsonify": lambda x: x,
                   "DATA_BRIDGE_BUILD": "fixture", "os": SimpleNamespace(environ={}),
                   "_option_underlying_quotes": lambda x: {},
                   "_psc_probe_snapshot_before": lambda *a, **kw: "20261013",
                   "_emsx_history_get_fills": lambda **kw: []})
        return ns

    def test_actual_endpoint_psc_failure_never_reports_complete_coverage(self):
        ns = self.endpoint_namespace()
        ns["_pyodbc_or_503"] = lambda: (None, "unavailable")
        result, status = ns["emsx_options_closeout_check"]()
        self.assertEqual(status, 200)
        self.assertEqual(result["coverage"]["status"], "incomplete")
        self.assertEqual(result["starting_positions_count"], 0)

    def test_actual_endpoint_quote_failure_preserves_known_action_warning(self):
        ns = self.endpoint_namespace()
        cursor = Cursor({"Alpha": [("QQQ 10/14/26 P729", "S", -250), ("SPY 10/14/26 C100", "L", 2)]})
        connection = SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
        ns["_pyodbc_or_503"] = lambda: (SimpleNamespace(connect=lambda *a, **kw: connection), None)
        ns["_emsx_history_get_fills"] = lambda **kw: [{"SECURITY": "QQQ261014P00729000", "ORDER_ID": "1", "ORDER_ACTION": "B", "ACT_QTTY": 250}]
        def no_quotes(_positions):
            raise RuntimeError("quotes unavailable")
        ns["_option_underlying_quotes"] = no_quotes
        result, status = ns["emsx_options_closeout_check"]()
        self.assertEqual(status, 200)
        self.assertEqual(result["coverage"]["status"], "incomplete")
        self.assertIn("quote_error", result["exercise_risk_status"])
        self.assertEqual(connection.timeout, 2)
        self.assertEqual(sum(g["flags_count"] for g in result["groups"]), 1)
        self.assertEqual(next(g["net_end"] for g in result["groups"] if g["security"].startswith("QQQ")), 0)


if __name__ == "__main__":
    unittest.main()
