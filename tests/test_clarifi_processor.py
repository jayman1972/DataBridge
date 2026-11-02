"""Offline checks for the Clarifi processor used by DataBridge routes."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

from clarifi_processor import process_clarifi_file


class ClarifiProcessorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.source = Path(self.temp_dir.name) / "MacroDataExport.csv"
        self.source.write_text(
            "date,median % change in actual eps q over q 1 yr ago\n"
            "2026-10-30,1.5\n",
            encoding="utf-8",
        )
        self.client = MagicMock()
        self.lookup = self.client.table.return_value.select.return_value.not_.is_.return_value.execute
        self.upsert = self.client.table.return_value.upsert.return_value.execute

    def test_lookup_failure_does_not_upsert_unknown_dates(self):
        self.lookup.side_effect = RuntimeError("database unavailable")

        result = process_clarifi_file(self.source, self.client)

        self.assertEqual(result["inserted"], 0)
        self.assertIn("Could not check existing dates", result["errors"][0])
        self.upsert.assert_not_called()

    def test_existing_date_is_not_rewritten(self):
        self.lookup.return_value.data = [{"date": "2026-10-30"}]

        result = process_clarifi_file(self.source, self.client)

        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["errors"], [])
        self.upsert.assert_not_called()

    def test_new_date_is_inserted_once(self):
        self.lookup.return_value.data = []

        result = process_clarifi_file(self.source, self.client)

        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["errors"], [])
        self.upsert.assert_called_once()

    def oil_source(self, body="10/30/2026,147.5933\n11/2/2026,119.6536\n"):
        self.source = self.source.with_name("OilDemand.csv")
        self.source.write_text("Start Date,1/1/2014\nEnd Date,\n,\n,index\n,Last Price\nDates,PX_LAST\n" + body, encoding="utf-8")

    def test_oil_sends_complete_delivery_without_skipping_existing_dates(self):
        self.oil_source()
        self.client.rpc.return_value.execute.return_value.data = {"versions": 2, "raw_changed": 1}
        result = process_clarifi_file(self.source, self.client)
        self.assertEqual(result["errors"], [])
        name, params = self.client.rpc.call_args.args
        self.assertEqual(name, "ingest_oil_demand_delivery")
        self.assertEqual(len(params["p_records"]), 2)
        self.assertEqual(params["p_file_text"], self.source.read_text(encoding="utf-8"))
        self.client.table.assert_not_called()

    def test_oil_rpc_failure_never_falls_back_to_raw_writes(self):
        self.oil_source()
        self.client.rpc.return_value.execute.side_effect = RuntimeError("unavailable")
        result = process_clarifi_file(self.source, self.client)
        self.assertIn("unavailable", result["errors"][0])
        self.client.table.assert_not_called()

    def test_invalid_oil_delivery_is_rejected_before_writing(self):
        for body in ("11/2/2026,nan\n", "11/2/2026,1\n11/2/2026,2\n", "11/2/2026,1,extra\n"):
            with self.subTest(body=body):
                self.oil_source(body)
                result = process_clarifi_file(self.source, self.client)
                self.assertTrue(result["errors"])
                self.client.rpc.assert_not_called()
                self.client.table.assert_not_called()


if __name__ == "__main__":
    unittest.main()
