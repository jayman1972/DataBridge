"""Verify the actual read SQL and access boundaries for security tax imports."""

import re
import sqlite3
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from sggg.non_option_tax import (
    FILL_COLUMNS,
    fetch_non_option_tax,
    register_non_option_tax_routes,
)


class NonOptionTaxTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.create_function("regexp", 2, lambda pattern, value:
                                  bool(re.search(pattern, value or "")))
        columns = [col.strip().split()[0].strip("`") for col in FILL_COLUMNS.split(",")]
        self.conn.execute("CREATE TABLE psc_filled_orders (" + ",".join(
            f"`{col}` {'INTEGER' if col == 'TRADE_DATE_INT' else 'TEXT'}" for col in columns
        ) + ")")
        self.conn.execute("CREATE TABLE psc_position_history "
                          "(PORTFOLIO TEXT, POSN_DATE INTEGER, SEC_CCY TEXT, FX_SECURITY_TO_BASE REAL, "
                          "STRATEGY TEXT, SECURITY TEXT, SECURITY_TYPE TEXT, COUNTRY TEXT, LONG_SHORT TEXT, "
                          "QUANTITY REAL, AVG_PRICE REAL, POSN_OPEN_DT INTEGER, BBG_TICKER TEXT, DESCRIPTION TEXT)")

    def add(self, name, day, kind="Stock", ticker=None, fund="Alpha"):
        self.conn.execute(
            "INSERT INTO psc_filled_orders "
            "(PORTFOLIO,SECURITY_SN,SECURITY,TRADE_DATE_INT,SECURITY_TYPE,BBG_TICKER,COMMISH_AMT) "
            "VALUES(?,?,?,?,?,?,?)", (fund, name, name, day, kind, ticker, "123.45"),
        )

    def test_preserves_opening_history_actual_commission_and_fund_end_boundaries(self):
        self.add("long", 20200102)
        self.add("long", 20261101)
        self.add("inactive", 20200102)
        self.add("later", 20261104)
        self.add("other", 20261101, fund="Other")
        self.add("cash", 20261101, kind="Cash")
        self.add("fx", 20261101, kind="FXForward")
        self.add("option", 20261101, kind="EquityOption")
        self.add("future option", 20261101, kind="Futures", ticker="CLU6C 85 Comdty")
        self.add("future", 20261101, kind="Futures", ticker="CLU6 Comdty")
        out = fetch_non_option_tax(self.conn, "Alpha", "2026-10-01", "2026-11-03")
        self.assertCountEqual([row["SECURITY"] for row in out["non_option_trades"]],
                              ["long", "long", "future"])
        self.assertEqual(out["non_option_trades"][0]["COMMISH_AMT"], "123.45")

    def test_requires_session_and_fund_admin_role_before_connecting(self):
        app = Flask(__name__)
        bridge = SimpleNamespace(SUPABASE_URL="https://example.invalid", SUPABASE_KEY="key")
        register_non_option_tax_routes(app, bridge=bridge)
        client = app.test_client()
        self.assertEqual(client.post("/sggg/non-option-tax-reconciliation").status_code, 401)
        with patch("sggg.non_option_tax.requests.get") as get:
            get.return_value.status_code = 401
            self.assertEqual(client.post("/sggg/non-option-tax-reconciliation",
                                         headers={"Authorization": "Bearer invalid"}).status_code, 403)


if __name__ == "__main__":
    unittest.main()
