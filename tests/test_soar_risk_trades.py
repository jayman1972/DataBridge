"""Weekly trade selection must respect the SOAR strategy booking."""

from __future__ import annotations

import sqlite3
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.soar_risk_data import _psc_weekly_trades


class SoarRiskTradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.psc = Mock()
        self.psc.cursor.return_value.execute.side_effect = (
            lambda query, *params: self.connection.execute(query, params)
        )
        self.connection.execute("""
            CREATE TABLE psc_filled_orders (
                TRADE_DATE_INT INTEGER, SECURITY TEXT, DESCRIPTION TEXT,
                `ORDER` TEXT, FILLED_QTTY REAL, PRICE REAL, BBG_TICKER TEXT,
                STRATEGY TEXT, ACCOUNT_NAME TEXT, SECURITY_SN INTEGER
            )
        """)

    def add_trade(
        self, symbol: str, strategy: str | None = "SOAR", *,
        day: int = 20261103, account: str = "BNS-SOAR LS",
        description: str = "ETF", ticker: str = "SPY US Equity",
    ) -> None:
        self.connection.execute(
            "INSERT INTO psc_filled_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (day, symbol, description, "BUY", 100, 50, ticker, strategy, account, 1),
        )

    def test_only_soar_strategy_trades_in_account_and_week_are_included(self) -> None:
        self.add_trade("SPY")
        self.add_trade("XLE", day=20261102)
        # Same security can be booked to different strategies.
        for strategy in ("Risk Arbitrage", "Issuance Arbitrage", "", None):
            self.add_trade("SPY", strategy)
        self.add_trade("OTHER_ACCOUNT", account="OTHER")
        self.add_trade("PRIOR_WEEK", day=20261030)
        self.add_trade("NEXT_WEEK", day=20261109)
        trades = _psc_weekly_trades(self.psc, date(2026, 11, 6))
        self.assertEqual([trade["symbol"] for trade in trades], ["XLE", "SPY"])
        self.assertEqual(trades[1]["shares"], 100)
        self.assertEqual(trades[1]["price"], 50)

    def test_currency_and_cash_remain_excluded_even_in_soar_strategy(self) -> None:
        self.add_trade("SPY")
        self.add_trade("CASH CAD")
        self.add_trade("CADUSD")
        self.add_trade("USDCAD.202611")
        self.add_trade("FX", ticker="EURUSD Curncy")
        self.add_trade("FORWARD", description="USD/CAD forward")
        trades = _psc_weekly_trades(self.psc, date(2026, 11, 6))
        self.assertEqual([trade["symbol"] for trade in trades], ["SPY"])

    def test_excluded_strategies_do_not_trigger_page_overflow(self) -> None:
        for _ in range(25):
            self.add_trade("SPY", "Risk Arbitrage")
        self.assertEqual(_psc_weekly_trades(self.psc, date(2026, 11, 6)), [])


if __name__ == "__main__":
    unittest.main()
