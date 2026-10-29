"""Saving a reviewed CSV never reloads NAV, transfers, holdings, or model data."""

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from sggg.production_routes import (
    register_sggg_production_routes,
    verify_recorded_trade_file,
)


class RecordedQuery:
    def __init__(self, records):
        self.records = records
        self.filters = {}

    def select(self, _columns):
        return self

    def eq(self, column, value):
        self.filters[column] = value
        return self

    def limit(self, _count):
        return self

    def execute(self):
        return SimpleNamespace(
            data=[
                record
                for record in self.records
                if all(record.get(key) == value for key, value in self.filters.items())
            ][:1]
        )


class RecordedClient:
    def __init__(self, table, owner, filename, digest):
        self.records = {
            table: [
                {
                    "id": "recorded-export",
                    owner: "user",
                    "filename": filename,
                    "payload_sha256": digest,
                }
            ]
        }
        self.queried = []

    def table(self, table):
        if table not in ("bt_sggg_trade_batches", "bt_sggg_trade_exports"):
            raise AssertionError(f"Save must not reload business data: {table}")
        self.queried.append(table)
        return RecordedQuery(self.records.get(table, []))


class ReviewedTradeSaveTest(unittest.TestCase):
    def test_ownership_and_digest_are_still_required(self):
        client = RecordedClient("bt_sggg_trade_batches", "created_by", "file", "hash")
        verify_recorded_trade_file(client, "user", "file", "hash")
        for user, filename, digest in (
            ("another-user", "file", "hash"),
            ("user", "another-file", "hash"),
            ("user", "file", "another-hash"),
        ):
            with self.assertRaisesRegex(ValueError, "owned by your account"):
                verify_recorded_trade_file(client, user, filename, digest)

    def test_both_reviewed_export_types_save_identical_bytes_without_capital_reads(self):
        filename = "Pending_Trade_Import_20261029.csv"
        csv = '\ufeffTrade Date,Symbol,Order Quantity,CUSIP\r\n2026-10-29,NVDA,71,"67066G104"'
        digest = hashlib.sha256(csv.encode("utf-8")).hexdigest()
        for table, owner in (
            ("bt_sggg_trade_batches", "created_by"),
            ("bt_sggg_trade_exports", "exported_by"),
        ):
            with self.subTest(table=table), tempfile.TemporaryDirectory() as directory:
                client = RecordedClient(table, owner, filename, digest)
                app = Flask(__name__)
                register_sggg_production_routes(
                    app, supabase_client=client, supabase_url="test", service_key="test"
                )
                with (
                    patch("sggg.production_routes._authenticated_admin_user_id", return_value=("user", None, 200)),
                    patch("sggg.fund_capital.fetch_fund_capital", side_effect=AssertionError("NAV must not be read")),
                    patch.dict(os.environ, {"SGGG_TRADE_EXPORT_DIR": directory}),
                ):
                    response = app.test_client().post(
                        "/sggg/alphadesk-trade-file",
                        json={"filename": filename, "csv": csv, "payload_sha256": digest},
                    )
                self.assertEqual(response.status_code, 200, response.get_json())
                self.assertEqual((Path(directory) / filename).read_bytes(), csv.encode("utf-8"))
                self.assertNotIn("bt_sggg_trade_drafts", client.queried)

    def test_missing_admin_session_never_writes(self):
        app = Flask(__name__)
        register_sggg_production_routes(
            app, supabase_client=object(), supabase_url="test", service_key="test"
        )
        with patch("sggg.production_routes.save_alphadesk_trade_file") as write:
            response = app.test_client().post("/sggg/alphadesk-trade-file", json={})
        self.assertEqual(response.status_code, 401)
        write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
