from __future__ import annotations

import unittest
from datetime import date

from src.bloomberg.etf_constituents import (
    fetch_etf_constituent_snapshots,
    next_weekday,
    normalize_etf_tickers,
    normalize_weight_rows,
)


class _Client:
    def __init__(self, weights: list[dict[str, object]]) -> None:
        self.weights = weights

    def get_reference_data(self, **_: object) -> dict[str, dict[str, str]]:
        return {"QQQ US Equity": {"FUND_BENCHMARK_PRIM": "NDX"}}

    def get_bds_rows(self, *_: object) -> dict[str, object]:
        return {"fields": {"INDX_MWEIGHT_HIST": self.weights}, "errors": []}


class BloombergEtfConstituentTests(unittest.TestCase):
    def test_normalizes_percent_weights(self) -> None:
        rows, weight_sum = normalize_weight_rows(
            [
                {"Index Member": "AAPL UW", "Percent Weight": 60},
                {"Index Member": "MSFT UW", "Percent Weight": 40},
            ]
        )
        self.assertEqual(weight_sum, 1)
        self.assertEqual(rows[0]["constituentWeight"], 0.6)

    def test_rejects_bloomberg_zero_weight_sentinel(self) -> None:
        with self.assertRaisesRegex(ValueError, "usable non-negative"):
            normalize_weight_rows(
                [
                    {"Index Member": "AAPL UW", "Percent Weight": -2.4e-14},
                    {"Index Member": "MSFT UW", "Percent Weight": -2.4e-14},
                ]
            )

    def test_fetch_resolves_benchmark_and_uses_next_weekday(self) -> None:
        result = fetch_etf_constituent_snapshots(
            _Client(
                [
                    {"Index Member": "AAPL UW", "Percent Weight": 60},
                    {"Index Member": "MSFT UW", "Percent Weight": 40},
                ]
            ),
            ["QQQ"],
            date(2026, 9, 11),
        )
        self.assertEqual(result["errors"], [])
        self.assertEqual(
            result["sourceProvider"], "bloomberg_benchmark_index_proxy"
        )
        self.assertEqual(result["usableDate"], "2026-09-14")
        self.assertEqual(result["snapshots"][0]["sourceIndexTicker"], "NDX Index")

    def test_request_is_bounded(self) -> None:
        self.assertEqual(normalize_etf_tickers(["qqq", "QQQ"]), ["QQQ"])
        with self.assertRaises(ValueError):
            normalize_etf_tickers(["QQQ US Equity"])

    def test_weekend_is_skipped(self) -> None:
        self.assertEqual(next_weekday(date(2026, 9, 11)), date(2026, 9, 14))


if __name__ == "__main__":
    unittest.main()
