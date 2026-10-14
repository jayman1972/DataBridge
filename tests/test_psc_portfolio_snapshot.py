"""Regression checks for the PSC portfolio query profiles."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import data_bridge


class FakeCursor:
    def __init__(self, rows, columns=()):
        self.rows = rows
        self.columns = columns
        self.queries = []

    def execute(self, sql, params=()):
        self.queries.append((sql, params))
        return self

    def fetchall(self):
        if "information_schema.COLUMNS" in self.queries[-1][0]:
            return self.columns
        return self.rows


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor

    def cursor(self):
        return self.fake_cursor

    def close(self):
        pass


class PortfolioSnapshotTests(unittest.TestCase):
    def get(self, cursor, url):
        fake_odbc = SimpleNamespace(connect=lambda _: FakeConnection(cursor))
        with patch.dict(sys.modules, {"pyodbc": fake_odbc}):
            return data_bridge.app.test_client().get(url)

    def test_production_snapshot_queries_only_required_fields(self):
        cursor = FakeCursor([(
            "EquityLO-US", "ABX", "Stock", "CAD", "ABX CN Equity", "LONG",
            100, 500.0, 5.0, 500.0, 2000.0,
        )])
        response = self.get(
            cursor, "/sggg/portfolio?date=20261013&fund=EHP%20Alpha&profile=production_snapshot"
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["fund_nav"], 2000.0)
        self.assertEqual(payload["positions"][0]["exposure_pct_nav"], 25.0)
        sql = cursor.queries[0][0]
        self.assertNotIn("psc_security_data", sql)
        self.assertNotIn("EXPOSURE_PCT_NAV", sql)
        self.assertNotIn("FX_EXPOSURE", sql)
        self.assertIn("ph.PORTFOLIO = ?", sql)
        self.assertEqual(cursor.queries[0][1], ("EHP Alpha", "20261013"))

    def test_general_profile_reports_missing_optional_columns(self):
        # Eleven dimensions, four optional NULLs, then the twelve portfolio measures.
        row = (
            "EquityLO-US", "group", "ABX", "Example", "Stock", "CAD", "ABX CN Equity",
            "Materials", "CA", "LONG", "SEDOL", None, None, None, None,
            100, 5, 5, 0, 1, 0, 0, 500, 500, 0, None, 2000,
        )
        columns = [("psc_position_history", "EXPOSURE"), ("psc_security_data", "SEDOL")]
        cursor = FakeCursor([row], columns)
        response = self.get(cursor, "/sggg/portfolio?date=20261013&fund=EHP%20Alpha")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("FX_EXPOSURE_DISPLAY_CCY", payload["missing_fields"])
        self.assertIsNone(payload["positions"][0]["fx_exposure"])
        self.assertIsNone(payload["positions"][0]["fx_exposure_pct_nav"])
        self.assertEqual(payload["positions"][0]["exposure_pct_nav"], 25.0)
        self.assertEqual(len(cursor.queries), 2)  # Metadata and portfolio.
        self.assertNotIn("FX_EXPOSURE_LOC", cursor.queries[1][0])

    def test_production_snapshot_rejects_invalid_nav(self):
        cursor = FakeCursor([("SOAR", "ABX", "Stock", "CAD", "ABX CN Equity", "LONG",
                              100, 500, 5, 500, None)])
        response = self.get(
            cursor, "/sggg/portfolio?date=20261013&fund=EHP%20Alpha&profile=production_snapshot"
        )
        self.assertEqual(response.status_code, 500)
        self.assertIn("fund NAV", response.get_json()["error"])

    def test_production_snapshot_never_uses_ambiguous_prefix_fallback(self):
        # A prefix such as EHP Alpha% could return EHP Alpha Hedge on a day
        # when the exact EHP Alpha portfolio has no positions.
        cursor = FakeCursor([])
        response = self.get(
            cursor, "/sggg/portfolio?date=20261013&fund=EHP%20Alpha&profile=production_snapshot"
        )
        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.get_json()["error"],
            "PSC portfolio query returned no rows for all SQL variants",
        )
        self.assertEqual(len(cursor.queries), 1)
        self.assertIn("ph.PORTFOLIO = ?", cursor.queries[0][0])


if __name__ == "__main__":
    unittest.main()
