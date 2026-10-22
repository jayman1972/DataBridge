"""Fund-level flows must be applied exactly once, never summed across positions."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from sggg.fund_capital import (
    apply_capital_basis,
    read_fund_capital,
    register_fund_capital_routes,
    verify_export_capital,
)
from sggg.production_routes import register_sggg_production_routes


class Cursor:
    def __init__(self, nav=34679651.6449, flows=-15016797.55):
        self.results = [[(2900,)], [(nav, flows)]]
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchall(self):
        return self.results.pop(0)


class AuditClient:
    def __init__(self, audit):
        self.tables = {
            "bt_sggg_trade_batches": [],
            "bt_sggg_trade_exports": [audit],
            "bt_exchange_sessions": [{"session_date": "2026-10-21", "is_open": True}],
            "bt_exchange_calendar_overrides": [],
            "bt_production_fund_rebalance_inputs": [],
        }

    def table(self, name):
        rows = self.tables[name]
        query = SimpleNamespace()
        for method in ("select", "eq", "gte", "lt", "order", "limit", "in_"):
            setattr(query, method, lambda *args, **kwargs: query)
        query.execute = lambda: SimpleNamespace(data=rows)
        return query


class FundCapitalTests(unittest.TestCase):
    def test_alpha_redemption(self):
        cursor = Cursor()
        capital = read_fund_capital(cursor, "EHP Alpha", "2026-10-21")
        self.assertAlmostEqual(capital["nav_after_flows"], 19662854.0949)
        self.assertEqual(cursor.calls[-1][1], (2900, 20261021))
        payload = apply_capital_basis(
            {
                "fund_nav": 34679651.6449,
                "nav_date": "2026-10-21",
                "positions": [{"exposure": 100}],
            },
            capital,
        )
        self.assertEqual(payload["fund_nav"], capital["nav_after_flows"])
        self.assertAlmostEqual(
            payload["positions"][0]["exposure_pct_nav"], 10000 / 19662854.0949
        )
        with self.assertRaises(ValueError):
            apply_capital_basis(payload, capital)  # A second application is rejected.

    def test_zero_and_subscription(self):
        for flows in (0, 1000):
            capital = read_fund_capital(Cursor(2000, flows), "EHP Alpha", "20261021")
            self.assertEqual(capital["nav_after_flows"], 2000 + flows)

    def test_missing_and_invalid_values_fail_closed(self):
        for nav, flows in (
            (None, 0),
            (100, None),
            (0, 0),
            (100, -100),
            (100, float("nan")),
        ):
            with self.subTest(nav=nav, flows=flows), self.assertRaises(ValueError):
                read_fund_capital(Cursor(nav, flows), "EHP Alpha", "20261021")
        for rows in ([], [(1,), (2,)]):
            cursor = Cursor()
            cursor.results = [rows, rows]
            with self.assertRaises(ValueError):
                read_fund_capital(cursor, "EHP Alpha", "20261021")

    def test_dates_and_funds(self):
        for fund, date in (("EHP Alpha Hedge", "20261021"), ("EHP Alpha", "20260230")):
            with self.assertRaises(ValueError):
                read_fund_capital(Cursor(), fund, date)

    def test_position_nav_mismatch(self):
        capital = read_fund_capital(Cursor(), "EHP Alpha", "20261021")
        for nav, date in ((123, "2026-10-21"), (34679651.6449, "2026-10-20")):
            with self.assertRaises(ValueError):
                apply_capital_basis({"fund_nav": nav, "nav_date": date}, capital)

    def test_endpoint_authorization_and_verification(self):
        app = Flask(__name__)
        register_fund_capital_routes(app, service_key="test")
        client = app.test_client()
        self.assertEqual(client.get("/sggg/fund-capital").status_code, 401)
        capital = read_fund_capital(Cursor(), "EHP Alpha", "20261021")
        with patch("sggg.fund_capital.fetch_fund_capital", return_value=capital):
            response = client.get(
                "/sggg/fund-capital?fund=EHP%20Alpha&date=20261021",
                headers={"Authorization": "Bearer test"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["capital_basis"], capital)

    def test_profile_only_enrichment_and_missing_flow_failure(self):
        app = Flask(__name__)
        capital = read_fund_capital(Cursor(), "EHP Alpha", "20261021")
        register_fund_capital_routes(app, service_key="test")

        @app.route("/sggg/portfolio", methods=["GET", "POST"])
        def portfolio():
            return {
                "fund": "EHP Alpha",
                "nav_date": "2026-10-21",
                "fund_nav": capital["nav_before_flows"],
                "positions": [],
            }

        client = app.test_client()
        with patch("sggg.fund_capital.fetch_fund_capital", return_value=capital):
            self.assertEqual(
                client.get("/sggg/portfolio").json["fund_nav"],
                capital["nav_before_flows"],
            )
            self.assertEqual(
                client.get("/sggg/portfolio?profile=production_snapshot").json[
                    "fund_nav"
                ],
                capital["nav_after_flows"],
            )
            self.assertEqual(
                client.post(
                    "/sggg/portfolio", json={"profile": "production_snapshot"}
                ).json["fund_nav"],
                capital["nav_after_flows"],
            )
        with patch(
            "sggg.fund_capital.fetch_fund_capital", side_effect=ValueError("Missing")
        ):
            self.assertEqual(
                client.get("/sggg/portfolio?profile=production_snapshot").status_code,
                422,
            )

    def test_server_export_rechecks_saved_proof_and_cash_adjustment(self):
        capital = read_fund_capital(Cursor(), "EHP Alpha", "20261021")
        saved = {
            "fundCapitalBasis": capital,
            "sourceNavCad": capital["nav_after_flows"],
            "navAdjustmentCad": 0,
            "adjustedNavCad": capital["nav_after_flows"],
        }
        audit = {
            "dashboard_fund_name": "EHP Alpha",
            "trade_date": "2026-10-22",
            "configuration_snapshot": saved,
        }
        client = AuditClient(audit)
        with patch("sggg.fund_capital.fetch_fund_capital", return_value=capital):
            verify_export_capital(client, "user", "file", "hash")
            client.tables["bt_production_fund_rebalance_inputs"] = [
                {"nav_adjustment_cad": -100}
            ]
            with self.assertRaisesRegex(ValueError, "adjustment changed"):
                verify_export_capital(client, "user", "file", "hash")
            client.tables["bt_production_fund_rebalance_inputs"] = []
            saved["fundCapitalBasis"] = {}
            with self.assertRaisesRegex(ValueError, "rerun"):
                verify_export_capital(client, "user", "file", "hash")
            client.tables["bt_sggg_trade_exports"] = []
            with self.assertRaisesRegex(ValueError, "audit is required"):
                verify_export_capital(client, "user", "file", "hash")

    def test_unverified_export_never_writes_a_file(self):
        app = Flask(__name__)
        register_sggg_production_routes(
            app, supabase_client=object(), supabase_url="test", service_key="test"
        )
        with (
            patch(
                "sggg.production_routes._authenticated_admin_user_id",
                return_value=("user", None, 200),
            ),
            patch(
                "sggg.production_routes.verify_export_capital",
                side_effect=ValueError("Rerun"),
            ),
            patch("sggg.production_routes.save_alphadesk_trade_file") as write,
        ):
            response = app.test_client().post(
                "/sggg/alphadesk-trade-file",
                json={"filename": "file", "csv": "csv", "payload_sha256": "hash"},
            )
        self.assertEqual(response.status_code, 422)
        write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
