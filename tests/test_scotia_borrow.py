"""Scotia routing, legacy CSV compatibility, transport and retry behavior."""

import copy
import hashlib
import io
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from sggg.production_routes import register_sggg_production_routes
from sggg.scotia_borrow import (
    DeliveryReviewRequired,
    LocateRow,
    deliver_scotia_locates,
    load_connection,
    load_scotia_rows,
    locate_csv,
    upload_file,
)

FIRST = "00000000-0000-0000-0000-000000000001"
SECOND = "00000000-0000-0000-0000-000000000002"
DAY = "2026-11-27"


def trade(**overrides):
    return {
        "side": "SS",
        "prime": "Scotia",
        "sgggPortfolio": "The Soar Fund",
        "tradeDate": DAY,
        "symbol": "AAPL",
        "orderQuantity": 123,
        "cusip": "037833100",
        "isin": "US0378331005",
        "sedol": "2046251",
        "custodianId": "AAPL.O",
        "currency": "USD",
        **overrides,
    }


class Query:
    def __init__(self, rows):
        self.rows = rows

    def select(self, _columns):
        return self

    def in_(self, column, values):
        self.rows = [row for row in self.rows if row[column] in values]
        return self

    def eq(self, column, value):
        self.rows = [row for row in self.rows if row[column] == value]
        return self

    def execute(self):
        return SimpleNamespace(data=self.rows)


class Client:
    def __init__(self):
        self.tables = {
            "bt_sggg_trade_drafts": [
                {
                    "id": FIRST,
                    "status": "ready",
                    "trade_date": DAY,
                    "dashboard_fund_name": "SOAR Fund",
                    "dashboard_strategy_name": "SOAR",
                    "source_run": {"status": "completed"},
                    "export_rows": [
                        trade(),
                        trade(side="BC"),
                        trade(side="B"),
                        trade(side="S"),
                    ],
                }
            ],
            "bt_sggg_trade_account_routes": [
                {
                    "active": True,
                    "dashboard_fund_name": "SOAR Fund",
                    "dashboard_strategy_name": None,
                    "prime": "Scotia",
                    "sggg_portfolio_name": "The Soar Fund",
                    "side_scope": "short",
                },
                {
                    "active": True,
                    "dashboard_fund_name": "EHP Strat Inc Alt",
                    "dashboard_strategy_name": "CreditMomentum",
                    "prime": "Scotia",
                    "sggg_portfolio_name": "EHP Strat Inc Alt",
                    "side_scope": "short",
                },
                {
                    "active": True,
                    "dashboard_fund_name": "EHP Alpha",
                    "dashboard_strategy_name": None,
                    "prime": "TD",
                    "sggg_portfolio_name": "EHP Alpha",
                    "side_scope": "both",
                },
            ],
        }

    def table(self, name):
        return Query(copy.deepcopy(self.tables[name]))


class ScotiaBorrowTest(unittest.TestCase):
    def test_exact_six_column_csv_only_short_sales_and_no_netting(self):
        client = Client()
        client.tables["bt_sggg_trade_drafts"][0]["export_rows"].append(
            trade(orderQuantity=7)
        )
        rows = load_scotia_rows(client, [FIRST], DAY)
        self.assertEqual(
            locate_csv(rows),
            b"CUSIP,ISIN,SEDOL,RIC,COUNTRY,QUANTITY\r\n037833100,US0378331005,2046251,AAPL.O,US,123\r\n037833100,US0378331005,2046251,AAPL.O,US,7\r\n",
        )

    def test_strategic_income_cad_and_td_exclusion(self):
        client = Client()
        draft = copy.deepcopy(client.tables["bt_sggg_trade_drafts"][0])
        draft.update(
            id=SECOND,
            dashboard_fund_name="EHP Alpha",
            export_rows=[trade(prime="TD", sgggPortfolio="EHP Alpha")],
        )
        client.tables["bt_sggg_trade_drafts"].append(draft)
        client.tables["bt_sggg_trade_drafts"][0].update(
            dashboard_fund_name="EHP Strat Inc Alt",
            dashboard_strategy_name="CreditMomentum",
            export_rows=[
                trade(currency="CAD", sgggPortfolio="EHP Strat Inc Alt", custodianId="")
            ],
        )
        rows = load_scotia_rows(client, [FIRST, SECOND], DAY)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].cells[3:], ("", "CA", "123"))

    def test_rejects_changed_routing_missing_drafts_or_failed_source(self):
        for changes in (
            {"status": "building"},
            {"trade_date": "2026-11-26"},
            {"source_run": {"status": "failed"}},
            {"dashboard_fund_name": "unknown"},
        ):
            client = Client()
            client.tables["bt_sggg_trade_drafts"][0].update(changes)
            with self.assertRaises(ValueError):
                load_scotia_rows(client, [FIRST], DAY)
        for ids in ([FIRST, FIRST], [SECOND], ["not-a-uuid"], [], [123]):
            with self.assertRaises(ValueError):
                load_scotia_rows(Client(), ids, DAY)

    def test_rejects_invalid_quantities_currency_and_identifiers(self):
        for changes in (
            {"orderQuantity": -1},
            {"orderQuantity": 1.5},
            {"orderQuantity": True},
            {"orderQuantity": float("nan")},
            {"currency": "EUR"},
            {"prime": "TD"},
            {"cusip": "=BAD"},
            {"isin": "bad\nvalue"},
            {"cusip": "", "isin": "", "sedol": "", "custodianId": ""},
        ):
            client = Client()
            client.tables["bt_sggg_trade_drafts"][0]["export_rows"] = [trade(**changes)]
            with self.assertRaises(ValueError):
                load_scotia_rows(client, [FIRST], DAY)

    def test_duplicate_and_overlapping_selection_does_not_resend(self):
        row = LocateRow(f"{FIRST}:0", ("037833100", "", "", "", "US", "123"))
        other = LocateRow(f"{SECOND}:0", ("037833100", "", "", "", "US", "7"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("sggg.scotia_borrow.upload_file") as upload:
                kwargs = {
                    "output_dir": root,
                    "audit_dir": root / "audit",
                    "connection_factory": fake_connection,
                }
                first = deliver_scotia_locates([row], DAY, "user", **kwargs)
                again = deliver_scotia_locates([row], DAY, "another-admin", **kwargs)
                overlap = deliver_scotia_locates([row, other], DAY, "user", **kwargs)
            self.assertEqual(upload.call_count, 2)
            self.assertEqual(
                first["filename"], "EDGEHILL_Scotia_Locates_NA_20261127_1.csv"
            )
            self.assertTrue(again["already_sent"])
            self.assertEqual(overlap["skipped_count"], 1)
            self.assertEqual(
                overlap["filename"], "EDGEHILL_Scotia_Locates_NA_20261127_2.csv"
            )
            self.assertEqual(
                (root / overlap["filename"]).read_bytes(), locate_csv([other])
            )
            changed = LocateRow(row.key, (*row.cells[:-1], "999"))
            with self.assertRaises(DeliveryReviewRequired):
                deliver_scotia_locates([changed], DAY, "user", **kwargs)

    def test_uncertain_delivery_is_reserved_and_not_retried(self):
        row = LocateRow("draft:0", ("037833100", "", "", "", "US", "123"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kwargs = {
                "output_dir": root,
                "audit_dir": root / "audit",
                "connection_factory": fake_connection,
            }
            with patch(
                "sggg.scotia_borrow.upload_file",
                side_effect=OSError("lost acknowledgement"),
            ) as upload:
                with (
                    self.assertLogs("data_bridge.sggg.scotia_borrow", level="ERROR"),
                    self.assertRaises(DeliveryReviewRequired),
                ):
                    deliver_scotia_locates([row], DAY, "user", **kwargs)
                with self.assertRaises(DeliveryReviewRequired):
                    deliver_scotia_locates([row], DAY, "user", **kwargs)
            self.assertEqual(upload.call_count, 1)
            with closing(sqlite3.connect(root / "audit/deliveries.sqlite3")) as audit:
                self.assertEqual(
                    audit.execute("SELECT status FROM deliveries").fetchone()[0],
                    "review",
                )

    def test_connect_failure_is_safe_to_retry_without_reservation(self):
        row = LocateRow("draft:0", ("037833100", "", "", "", "US", "123"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kwargs = {"output_dir": root, "audit_dir": root / "audit"}
            with self.assertRaises(OSError):
                deliver_scotia_locates(
                    [row], DAY, "user", connection_factory=failed_connection, **kwargs
                )
            with patch("sggg.scotia_borrow.upload_file"):
                result = deliver_scotia_locates(
                    [row], DAY, "user", connection_factory=fake_connection, **kwargs
                )
            self.assertEqual(result["row_count"], 1)

    def test_upload_verifies_temp_size_then_non_overwriting_rename(self):
        class Buffer(io.BytesIO):
            def close(self):
                pass

        class Sftp:
            def __init__(self):
                self.output = Buffer()
                self.operations = []

            def open(self, path, mode):
                self.operations.append((path, mode))
                return self.output

            def stat(self, _path):
                return SimpleNamespace(st_size=len(self.output.getvalue()))

            def rename(self, source, target):
                self.operations.append((source, target))

        sftp = Sftp()
        upload_file(sftp, "test.csv", b"test")
        self.assertEqual(
            sftp.operations,
            [
                ("/drop/SL/test.csv.filepart", "wx"),
                ("/drop/SL/test.csv.filepart", "/drop/SL/test.csv"),
            ],
        )

    def test_configuration_rejects_another_server_without_exposing_password(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transfer.txt"
            path.write_text(
                "open sftp://user:secret@other.example\nput * /drop/SL/",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError, "configured Scotia SFTP server"
            ) as error:
                load_connection(path)
            self.assertNotIn("secret", str(error.exception))

    def test_route_requires_admin_and_uses_only_saved_draft_rows(self):
        app = Flask(__name__)
        register_sggg_production_routes(
            app, supabase_client=Client(), supabase_url="test", service_key="test"
        )
        with patch("sggg.production_routes.deliver_scotia_locates") as deliver:
            self.assertEqual(
                app.test_client()
                .post("/sggg/scotia-borrow-requests", json={})
                .status_code,
                401,
            )
            deliver.assert_not_called()
        with (
            patch(
                "sggg.production_routes._authenticated_admin_user_id",
                return_value=("user", None, 200),
            ),
            patch(
                "sggg.production_routes.deliver_scotia_locates",
                return_value={"success": True, "row_count": 1},
            ) as deliver,
        ):
            response = app.test_client().post(
                "/sggg/scotia-borrow-requests",
                json={
                    "draft_ids": [FIRST],
                    "trade_date": DAY,
                    "payload_sha256": hashlib.sha256(
                        locate_csv(load_scotia_rows(Client(), [FIRST], DAY))
                    ).hexdigest(),
                    "csv": "UNTRUSTED",
                    "orderQuantity": 999999,
                },
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(deliver.call_args.args[0][0].cells[-1], "123")
            changed = app.test_client().post(
                "/sggg/scotia-borrow-requests",
                json={
                    "draft_ids": [FIRST],
                    "trade_date": DAY,
                    "payload_sha256": "wrong",
                },
            )
            self.assertEqual(changed.status_code, 422)
            self.assertEqual(deliver.call_count, 1)


@contextmanager
def fake_connection():
    yield object()


@contextmanager
def failed_connection():
    raise OSError("Cannot connect")
    yield  # pragma: no cover


if __name__ == "__main__":
    unittest.main()
