"""Exercise the actual report SQL against mixed AlphaDesk security types."""

import ast
import re
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sggg.options_tax_scope import OPTION_TRADE_SCOPE


class OptionsTaxScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse((ROOT / "data_bridge.py").read_text(encoding="utf-8-sig"))
        route = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name == "sggg_options_tax_reconciliation")
        cls.queries = {}
        for node in ast.walk(route):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in {
                        "orders_sql_base", "distinct_sql",
                    }:
                        cls.queries[target.id] = eval(compile(
                            ast.Expression(node.value), "report_query", "eval"
                        ), {"OPTION_TRADE_SCOPE": OPTION_TRADE_SCOPE})

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.create_function("regexp", 2, lambda pattern, value:
                                value is not None and re.search(pattern, value) is not None)
        columns = self.queries["orders_sql_base"].split("FROM", 1)[0][7:].split(",")
        names = [column.strip().split()[0].strip("`") for column in columns]
        self.db.execute("CREATE TABLE psc_filled_orders (" + ",".join(
            f"`{name}` TEXT" for name in names
        ) + ")")

    def add(self, name, security_type, ticker, portfolio="EHP Alpha", day="20260319"):
        self.db.execute(
            "INSERT INTO psc_filled_orders (SECURITY,SECURITY_TYPE,BBG_TICKER,PORTFOLIO,TRADE_DATE_INT) "
            "VALUES (?,?,?,?,?)", (name, security_type, ticker, portfolio, day),
        )

    def test_mislabelled_oil_and_index_options_are_included_without_outright_futures(self):
        self.add("oil call", "Futures", "COM6C 98.25 US Comdty")
        self.add("oil put", "Futures", "COM6P 75 US Comdty")
        self.add("index option", "Futures", "NQU6P 27000 Index")
        self.add("pricing source", "Futures", "CLQ6C 77.50 COMB Comdty")
        self.add("ordinary option", "EquityOption", "QQQ US 03/19/26 P600 Equity")
        self.add("oil future", "Futures", "COM6 Comdty")
        self.add("index future", "Futures", "NQU6 Index")
        self.add("stock", "Equity", "CL US Equity")
        self.add("unknown future", "Futures", None)
        rows = self.db.execute(self.queries["orders_sql_base"],
                               ("20260101", "20261103", "EHP Alpha")).fetchall()
        self.assertEqual({row[2] for row in rows}, {
            "oil call", "oil put", "index option", "pricing source", "ordinary option",
        })

    def test_both_queries_retain_fund_and_date_boundaries(self):
        self.add("inside", "Futures", "COM6C 98.25 US Comdty")
        self.add("other fund", "Futures", "COM6C 98.25 US Comdty", portfolio="Other")
        self.add("outside", "Futures", "COM6C 98.25 US Comdty", day="20251201")
        rows = self.db.execute(self.queries["orders_sql_base"],
                               ("20260101", "20261103", "EHP Alpha")).fetchall()
        self.assertEqual([row[2] for row in rows], ["inside"])
        portfolios = self.db.execute(self.queries["distinct_sql"],
                                     ("20260101", "20261103", "EHP%")).fetchall()
        self.assertEqual(portfolios, [("EHP Alpha",)])


if __name__ == "__main__":
    unittest.main()
