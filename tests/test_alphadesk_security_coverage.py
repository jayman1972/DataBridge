from __future__ import annotations

import unittest

from sggg.alphadesk_security_coverage import (
    fetch_alphadesk_security_coverage,
    is_valid_sedol,
)


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.parameters = ()

    def execute(self, sql, parameters):
        self.sql = sql
        self.parameters = parameters

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)

    def cursor(self):
        return self.cursor_instance


def _row(
    *,
    symbol="MSFT",
    cusip="594918104",
    isin="US5949181045",
    sedol="2588173",
    bloomberg="MSFT US Equity",
):
    return (
        symbol,
        "Microsoft Corp",
        "USD",
        cusip,
        isin,
        sedol,
        bloomberg,
        symbol,
        "United States",
        "NASDAQ",
    )


class AlphaDeskSecurityCoverageTests(unittest.TestCase):
    def test_validates_sedol_checksum(self):
        self.assertTrue(is_valid_sedol("2588173"))
        self.assertTrue(is_valid_sedol("BPCQCP9"))
        self.assertFalse(is_valid_sedol("2588174"))
        self.assertFalse(is_valid_sedol(""))

    def test_distinguishes_ready_missing_sedol_and_missing_security(self):
        connection = _Connection(
            [
                _row(),
                _row(
                    symbol="SHOP",
                    cusip="82509L107",
                    isin="CA82509L1076",
                    sedol="",
                    bloomberg="SHOP CN Equity",
                ),
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [
                {"symbol": "MSFT", "cusip": "594918104"},
                {"symbol": "SHOP.TO", "isin": "CA82509L1076"},
                {"symbol": "NEW", "sedol": "BD0Y0Q0"},
            ],
        )
        self.assertEqual(
            [row["status"] for row in result["securities"]],
            ["ready", "missing_or_invalid_sedol", "missing_security"],
        )
        self.assertIn("UPPER(TRIM(COALESCE(CUSIP, '')))", connection.cursor_instance.sql)

    def test_prefers_identifier_match_over_symbol_only_match(self):
        connection = _Connection(
            [
                _row(symbol="DUPE", cusip="", isin="", sedol=""),
                _row(symbol="OTHER", cusip="594918104"),
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [{"symbol": "DUPE", "cusip": "594918104"}],
        )
        self.assertEqual(result["securities"][0]["alphadesk_symbol"], "OTHER")
        self.assertEqual(result["securities"][0]["status"], "ready")


if __name__ == "__main__":
    unittest.main()
