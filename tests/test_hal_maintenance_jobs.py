"""Offline tests for the HAL maintenance caller's safety gates."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import hal_maintenance_jobs as jobs


def make_csv(path, latest="10/30/2026,147.5933"):
    path.write_text(
        "Start Date,1/1/2014\nEnd Date,\n,\n,.OILMKTBL U Index\n,Last Price\nDates,PX_LAST\n"
        "10/29/2026,139.9835\n" + latest + "\n",
        encoding="utf-8",
    )


def completed_run(run_id, job_key, *, success=True, warnings=0, oil=True):
    if job_key == "clarifi":
        files = [{"file": "OilDemand.csv", "errors": [], "inserted": 1}] if oil else []
        results = {
            "Clarifi Data": {"errors": [], "debug": {"files_processed": files}},
            "EHP Files": {
                "errors": [],
                "debug": {
                    "files_processed": [
                        {"file": "example.txt", "found": True, "size": 100}
                    ]
                },
            },
        }
    else:
        results = {
            "CalculatedFields": {
                "errors": [],
                "debug": {"steps": [{"step": "updates_complete"}]},
            }
        }
    return {
        "id": run_id,
        "job_key": job_key,
        "completed_at": "2026-10-31T15:00:00Z",
        "success": success,
        "skipped": False,
        "message": "Completed",
        "details": {
            "results": results,
            "warnings": warnings > 0,
            "warningCount": warnings,
            "errorCount": 0,
        },
    }


class FakeClient:
    _key = "test-secret"

    def __init__(self):
        self.starts = []
        self.latest_runs = {}
        self.runs = {
            101: completed_run(101, "clarifi"),
            102: completed_run(102, "calculated-fields"),
        }
        self.rows = [
            {
                "date": "2026-10-30",
                "oil_supply_demand_index": 147.5933,
                "oil_supply_demand_indicator": 1,
            },
            {
                "date": "2026-10-29",
                "oil_supply_demand_index": 139.9835,
                "oil_supply_demand_indicator": 0,
            },
        ]

    def latest(self, job_key):
        return self.latest_runs.get(job_key)

    def start(self, job_key):
        self.starts.append(job_key)
        return {"ok": True, "runId": 101 if job_key == "clarifi" else 102}

    def run(self, run_id, job_key):
        run = self.runs[run_id]
        assert run["job_key"] == job_key
        return run

    def oil_rows(self):
        return self.rows


class HalMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.csv_path = self.root / "OilDemand.csv"
        self.state_path = self.root / "state.json"
        make_csv(self.csv_path)

    def test_sequential_run_and_notification_dedup(self):
        client = FakeClient()
        first = jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(client.starts, ["clarifi", "calculated-fields"])
        self.assertEqual(first["runIds"], {"clarifi": 101, "calculated-fields": 102})
        self.assertTrue(first["notify"])
        self.assertEqual(first["signal"]["transitionId"], "2026-10-30:0>1")

        second = jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertTrue(second["alreadyComplete"])
        self.assertFalse(second["notify"])
        self.assertFalse(second["signal"]["notify"])
        self.assertEqual(len(client.starts), 2)

    def test_failed_clarifi_does_not_start_calculated_fields(self):
        client = FakeClient()
        client.runs[101] = completed_run(101, "clarifi", warnings=1)
        with self.assertRaisesRegex(jobs.JobError, "material warnings"):
            jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(client.starts, ["clarifi"])
        self.assertEqual(
            jobs.status(client, self.state_path)["runs"]["clarifi"]["warningCount"], 1
        )

    def test_uncertain_start_is_not_retried(self):
        class UncertainClient(FakeClient):
            def start(self, job_key):
                self.starts.append(job_key)
                raise jobs.JobError("Request outcome uncertain")

        client = UncertainClient()
        with self.assertRaisesRegex(jobs.JobError, "uncertain"):
            jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        with self.assertRaisesRegex(jobs.JobError, "uncertain"):
            jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(client.starts, ["clarifi"])

        client.latest_runs["clarifi"] = {
            "id": 101,
            "completedAt": "2026-10-31T15:00:00Z",
        }
        client.start = FakeClient.start.__get__(client)
        recovered = jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(recovered["runIds"]["clarifi"], 101)
        self.assertEqual(client.starts, ["clarifi", "calculated-fields"])

    def test_stale_oil_value_blocks_notification_without_restarting_jobs(self):
        client = FakeClient()
        client.rows[0]["oil_supply_demand_index"] = 147.0
        with self.assertRaisesRegex(jobs.JobError, "does not match"):
            jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(client.starts, ["clarifi", "calculated-fields"])
        client.rows[0]["oil_supply_demand_index"] = 147.5933
        recovered = jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertTrue(recovered["notify"])
        self.assertEqual(len(client.starts), 2)

    def test_signal_requires_explicit_binary_state(self):
        observation = jobs.oil_csv_observation(self.csv_path)
        for indicator in (None, True, "1", 2):
            with self.subTest(indicator=indicator):
                rows = FakeClient().rows
                rows[0]["oil_supply_demand_indicator"] = indicator
                with self.assertRaisesRegex(jobs.JobError, "explicit 0/1"):
                    jobs.oil_transition(rows, observation, None)

    def test_status_rejects_unrecorded_run_id(self):
        with self.assertRaisesRegex(jobs.JobError, "not recorded"):
            jobs.status(FakeClient(), self.state_path, 999)

    def test_client_uses_only_manual_job_actions_and_fresh_oil_rows(self):
        client = jobs.MaintenanceClient("test-secret")
        calls = []

        def request(url, body=None):
            calls.append((url, body))
            if body and body["action"] == "startJob":
                return {"ok": True, "runId": 101}
            return []

        client._request = request
        self.assertEqual(client.start("clarifi")["runId"], 101)
        client.oil_rows()
        self.assertEqual(calls[0][1], {"action": "startJob", "jobKey": "clarifi"})
        self.assertIn("oil_supply_demand_index=not.is.null", calls[1][0])
        with self.assertRaisesRegex(jobs.JobError, "Unsupported maintenance job"):
            client.start("other-job")

    def test_ehp_files_must_be_present(self):
        run = completed_run(101, "clarifi")
        run["details"]["results"]["EHP Files"]["debug"]["files_processed"] = []
        with self.assertRaisesRegex(jobs.JobError, "EHP files"):
            jobs.ensure_success(run, "clarifi")

    def test_handler_change_requires_review(self):
        client = jobs.MaintenanceClient("test-secret")
        client.action = lambda _action: {
            "jobs": [
                {
                    "jobKey": "calculated-fields",
                    "enabled": True,
                    "manualTriggerAllowed": True,
                    "handlerTarget": "client-orchestrator",
                    "handlerBody": {"orchestrator": "full_backfill"},
                }
            ]
        }
        with self.assertRaisesRegex(jobs.JobError, "handler configuration changed"):
            client.job("calculated-fields")

    def test_explicit_retry_of_completed_failed_clarifi(self):
        client = FakeClient()
        client.runs[101] = completed_run(101, "clarifi", success=False)
        with self.assertRaisesRegex(jobs.JobError, "failed"):
            jobs.execute(client, self.state_path, self.csv_path, 1, 5)
        self.assertEqual(
            jobs.retry_failed(client, self.state_path, "clarifi"),
            {"retryReady": "clarifi", "previousRunId": 101},
        )
        self.assertEqual(jobs.load_state(self.state_path)["cycle"]["runs"], {})

    def test_uncertain_start_cannot_be_reset_as_failed(self):
        observation = jobs.oil_csv_observation(self.csv_path)
        jobs.save_state(
            self.state_path,
            {
                "cycle": {
                    **observation,
                    "phase": "running",
                    "runs": {"clarifi": {"starting": "2026-10-31", "baseline": 100}},
                }
            },
        )
        with self.assertRaisesRegex(jobs.JobError, "uncertain start"):
            jobs.retry_failed(FakeClient(), self.state_path, "clarifi")

    def test_warning_messages_are_reported_without_exposing_key(self):
        run = completed_run(101, "clarifi")
        run["details"]["results"]["EHP Files"]["warnings"] = "check test-secret file"
        summary = jobs.run_summary(run, "test-secret")
        self.assertEqual(summary["warnings"], ["EHP Files: check [redacted] file"])


if __name__ == "__main__":
    unittest.main()
