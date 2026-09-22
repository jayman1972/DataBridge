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
    security=None,
    description="Microsoft Corp",
    cusip="594918104",
    isin="US5949181045",
    sedol="2588173",
    bloomberg="MSFT US Equity",
    country="United States",
    exchange="NASDAQ",
):
    return (
        security or f"{symbol}.US",
        description,
        "USD",
        cusip,
        isin,
        sedol,
        bloomberg,
        symbol,
        country,
        exchange,
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
                {"symbol": "MSFT", "cusip": "594918104", "sedol": "2588173"},
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
            [{"symbol": "DUPE", "cusip": "594918104", "sedol": "2588173"}],
        )
        self.assertEqual(result["securities"][0]["alphadesk_symbol"], "OTHER")
        self.assertEqual(result["securities"][0]["status"], "ready")

    def test_matches_tsx_symbol_to_alphadesk_ca_listing(self):
        connection = _Connection(
            [
                _row(
                    symbol="ABX",
                    security="ABX.CA",
                    description="Barrick Mining Corp",
                    cusip="06849F108",
                    isin="CA06849F1080",
                    sedol="BNM23Q1",
                    bloomberg="ABX CN Equity",
                    country="CA",
                    exchange="TSX",
                ),
                _row(
                    symbol="ABX",
                    security="ABX.US",
                    description="Abacus Global Management Inc",
                    cusip="00258Y104",
                    isin="US00258Y1047",
                    sedol="BRK3GP4",
                    bloomberg="ABX US Equity",
                ),
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [
                {
                    "symbol": "ABX.TO",
                    "bloomberg_symbol": "ABX CN Equity",
                    "cusip": "06849F108",
                    "isin": "CA06849F1080",
                    "sedol": "BNM23Q1",
                }
            ],
        )
        coverage = result["securities"][0]
        self.assertEqual(coverage["status"], "ready")
        self.assertEqual(coverage["alphadesk_security"], "ABX.CA")
        self.assertIn("ABX.CA", connection.cursor_instance.parameters)

    def test_reports_stale_identifiers_against_exact_listing(self):
        connection = _Connection(
            [
                _row(
                    symbol="ABX",
                    security="ABX.CA",
                    description="Barrick Mining Corp",
                    cusip="06849F108",
                    isin="CA06849F1080",
                    sedol="BNM23Q1",
                    bloomberg="ABX CN Equity",
                ),
                _row(
                    symbol="GOLD",
                    security="GOLD.US",
                    description="Barrick Gold Corp",
                    cusip="067901108",
                    isin="CA0679011084",
                    sedol="",
                    bloomberg="GOLD US Equity",
                ),
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [
                {
                    "symbol": "ABX.TO",
                    "bloomberg_symbol": "ABX CN Equity",
                    "cusip": "067901108",
                    "isin": "CA0679011084",
                    "sedol": "2024644",
                }
            ],
        )
        coverage = result["securities"][0]
        self.assertEqual(coverage["status"], "identity_conflict")
        self.assertEqual(coverage["alphadesk_security"], "ABX.CA")
        self.assertEqual(
            [conflict["field"] for conflict in coverage["conflicting_fields"]],
            ["cusip", "isin", "sedol"],
        )

    def test_does_not_silently_choose_between_cross_listings(self):
        connection = _Connection(
            [
                _row(
                    symbol="B",
                    security="B.US",
                    cusip="06849F108",
                    isin="CA06849F1080",
                    sedol="BNKB6Z0",
                    bloomberg="B US Equity",
                ),
                _row(
                    symbol="ABX",
                    security="ABX.CA",
                    cusip="06849F108",
                    isin="CA06849F1080",
                    sedol="BNM23Q1",
                    bloomberg="ABX CN Equity",
                ),
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [{"symbol": "UNKNOWN", "cusip": "06849F108", "isin": "CA06849F1080"}],
        )
        self.assertEqual(
            result["securities"][0]["status"],
            "ambiguous_security",
        )

    def test_blank_sedol_is_not_ready_when_alphadesk_holds_one(self):
        # The trade file carries our SEDOL, not AlphaDesk's. Every permanent
        # identifier we do hold agrees, so this is the right listing, but the
        # row would still go to the administrator with no SEDOL at all.
        connection = _Connection(
            [
                _row(
                    symbol="CCL",
                    description="Carnival Corp Ltd",
                    cusip="G2004J103",
                    isin="BMG2004J1036",
                    sedol="BVV7RC7",
                    bloomberg="CCL US Equity",
                )
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [
                {
                    "symbol": "CCL",
                    "bloomberg_symbol": "CCL US Equity",
                    "cusip": "G2004J103",
                    "isin": "BMG2004J1036",
                    "sedol": "",
                }
            ],
        )
        coverage = result["securities"][0]
        self.assertEqual(coverage["status"], "identity_conflict")
        self.assertEqual(
            coverage["conflicting_fields"],
            [{"field": "sedol", "requested": "", "alphadesk": "BVV7RC7"}],
        )

    def test_matching_sedol_on_the_exact_listing_is_ready(self):
        connection = _Connection(
            [
                _row(
                    symbol="CCL",
                    description="Carnival Corp Ltd",
                    cusip="G2004J103",
                    isin="BMG2004J1036",
                    sedol="BVV7RC7",
                    bloomberg="CCL US Equity",
                )
            ]
        )
        result = fetch_alphadesk_security_coverage(
            connection,
            [
                {
                    "symbol": "CCL",
                    "bloomberg_symbol": "CCL US Equity",
                    "cusip": "G2004J103",
                    "isin": "BMG2004J1036",
                    "sedol": "BVV7RC7",
                }
            ],
        )
        self.assertEqual(result["securities"][0]["status"], "ready")


if __name__ == "__main__":
    unittest.main()
