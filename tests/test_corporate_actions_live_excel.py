"""Explicit opt-in live integration test; never runs in the ordinary unit suite."""

from __future__ import annotations

import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from flask import Flask

from bloomberg.blpapi_client import BLPAPIClient
from bloomberg.corporate_actions_jobs import JobManager
from bloomberg.corporate_actions_routes import register_corporate_action_routes


@unittest.skipUnless(
    os.environ.get("DATABRIDGE_LIVE_EXCEL_TEST") == "YES",
    "Live Bloomberg Excel test requires explicit opt-in",
)
class LiveExcelJobTests(unittest.TestCase):
    def test_authenticated_job_returns_verified_historical_events(self):
        with tempfile.TemporaryDirectory() as folder:
            manager = JobManager(Path(folder), BLPAPIClient())
            app = Flask(__name__)
            register_corporate_action_routes(
                app,
                client=manager.client,
                service_key="live-test-only",
                job_manager=manager,
            )
            headers = {
                "Authorization": "Bearer live-test-only",
                "X-Requester-Id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            }
            payload = {
                "operation": "start",
                "request_id": str(uuid.uuid4()),
                "request": {
                    "securities": ["META US Equity", "TWTR US Equity", "ARM US Equity"],
                    "scope": "security",
                    "date_basis": "effective",
                    "action_types": ["listings", "delistings", "ticker_changes"],
                    "start_date": "2022-01-01",
                    "end_date": "2023-12-31",
                },
            }
            try:
                with app.test_client() as client:
                    response = client.post(
                        "/bloomberg/corporate-actions-jobs",
                        json=payload,
                        headers=headers,
                    )
                    self.assertEqual(response.status_code, 202, response.json)
                    job_id = response.json["job_id"]
                    deadline = time.monotonic() + 180
                    while time.monotonic() < deadline:
                        response = client.post(
                            "/bloomberg/corporate-actions-jobs",
                            json={"operation": "status", "job_id": job_id},
                            headers=headers,
                        )
                        self.assertEqual(response.status_code, 200, response.json)
                        if response.json["status"] in {
                            "completed",
                            "failed",
                            "cancelled",
                        }:
                            break
                        time.sleep(1)
                    self.assertEqual(
                        response.json["status"], "completed", response.json
                    )
                    result = response.json["result"]
                    self.assertEqual(result["errors"], [])
                    found = {
                        (row["action_type"], row["date"]) for row in result["events"]
                    }
                    self.assertTrue(
                        {
                            ("listings", "2023-09-14"),
                            ("delistings", "2022-10-28"),
                            ("ticker_changes", "2022-06-09"),
                        }.issubset(found),
                        result,
                    )
                    self.assertFalse(result["coverage"]["complete_cact"])
                    self.assertEqual(result["request"], payload["request"])
            finally:
                manager.close()


if __name__ == "__main__":
    unittest.main()
