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


if __name__ == "__main__":
    unittest.main()
