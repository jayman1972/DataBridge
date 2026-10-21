from __future__ import annotations

import json
import tempfile
import threading
import unittest
import uuid
import zipfile
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from flask import Flask

from bloomberg.corporate_actions_bql import PARTIAL_COVERAGE
from bloomberg.corporate_actions_excel_worker import TEMPLATE, OwnedExcel
from bloomberg.corporate_actions_jobs import JobManager, QueueFullError, hybrid_lookup
from bloomberg.corporate_actions_routes import register_corporate_action_routes

OWNER = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
OTHER = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def options(**changes):
    return {
        "securities": ["META US Equity"],
        "scope": "security",
        "date_basis": "effective",
        "action_types": ["ticker_changes"],
        "start_date": "2022-01-01",
        "end_date": "2022-12-31",
        **changes,
    }


class JobTests(unittest.TestCase):
    def test_owner_isolation_idempotency_and_persistent_result(self):
        with tempfile.TemporaryDirectory() as folder:
            gate = threading.Event()
            runner = Mock(
                side_effect=lambda *args: (
                    gate.wait(2) and {"events": [], "errors": [], "warnings": []}
                )
            )
            manager = JobManager(Path(folder), object(), runner=runner)
            request_id = str(uuid.uuid4())
            try:
                first = manager.create(OWNER, request_id, options())
                second = manager.create(OWNER, request_id, options())
                self.assertEqual(first["job_id"], second["job_id"])
                with self.assertRaises(KeyError):
                    manager.get(OTHER, first["job_id"])
                with self.assertRaises(KeyError):
                    manager.cancel(OTHER, first["job_id"])
                with self.assertRaises(ValueError):
                    manager.create(OWNER, request_id, options(scope="issuer"))
            finally:
                gate.set()
                manager.close()
            self.assertEqual(runner.call_count, 1)
            reopened = JobManager(Path(folder), object())
            try:
                self.assertEqual(
                    reopened.get(OWNER, first["job_id"])["status"], "completed"
                )
                self.assertEqual(
                    reopened.get(OWNER, first["job_id"])["result"]["events"], []
                )
            finally:
                reopened.close()

    def test_bounded_queue_and_queued_cancellation(self):
        with tempfile.TemporaryDirectory() as folder:
            gate = threading.Event()
            calls = []

            def runner(*args):
                calls.append(args)
                gate.wait(2)
                return {"events": []}

            manager = JobManager(Path(folder), object(), runner=runner)
            try:
                jobs = [
                    manager.create(OWNER, str(uuid.uuid4()), options())
                    for _ in range(4)
                ]
                with self.assertRaises(QueueFullError):
                    manager.create(OWNER, str(uuid.uuid4()), options())
                self.assertEqual(
                    manager.cancel(OWNER, jobs[-1]["job_id"])["status"], "cancelled"
                )
            finally:
                gate.set()
                manager.close()
            self.assertEqual(len(calls), 3)

    def test_running_cancellation_and_failed_job_are_visible(self):
        with tempfile.TemporaryDirectory() as folder:
            started, stopped = threading.Event(), threading.Event()

            def runner(_client, _options, _folder, progress, cancelled):
                started.set()
                stopped.wait(2)
                progress("test progress")
                self.assertTrue(cancelled())
                return {"events": [{"id": "completed-query"}]}

            manager = JobManager(Path(folder), object(), runner=runner)
            job = manager.create(OWNER, str(uuid.uuid4()), options())
            try:
                self.assertTrue(started.wait(2))
                manager.cancel(OWNER, job["job_id"])
            finally:
                stopped.set()
                manager.close()
            result = manager.get(OWNER, job["job_id"])
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(len(result["result"]["events"]), 1)

    def test_restart_does_not_leave_running_jobs_forever(self):
        with tempfile.TemporaryDirectory() as folder:
            manager = JobManager(Path(folder), object())
            manager.close()
            with closing(manager.connect()) as db, db:
                db.execute(
                    "INSERT INTO jobs(id,owner,request_id,options,status,message,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        "1" * 32,
                        OWNER,
                        str(uuid.uuid4()),
                        json.dumps(options()),
                        "running",
                        "test",
                        1,
                        1,
                    ),
                )
            reopened = JobManager(Path(folder), object())
            try:
                self.assertEqual(reopened.get(OWNER, "1" * 32)["status"], "failed")
                self.assertIn("restarted", reopened.get(OWNER, "1" * 32)["error"])
            finally:
                reopened.close()

    def test_hybrid_keeps_partial_api_data_when_excel_fails(self):
        base = {
            "events": [],
            "errors": [],
            "warnings": ["API-derived lookup, old warning"],
            "mapping": [],
            "request": {},
            "queried_count": 0,
            "expanded_count": 1,
            "resolved_universe": {"META US Equity": ["META US Equity"]},
        }
        with (
            patch("bloomberg.corporate_actions_jobs.lookup", return_value=base),
            patch(
                "bloomberg.corporate_actions_jobs.run_excel_job",
                side_effect=RuntimeError("Excel unavailable"),
            ),
        ):
            result = hybrid_lookup(
                object(),
                options(scope="security_plus"),
                Path("unused"),
                Mock(),
                lambda: False,
            )
        self.assertEqual(result["warnings"][0], PARTIAL_COVERAGE)
        self.assertTrue(result["errors"])
        self.assertNotIn("resolved_universe", result)
        self.assertFalse(result["coverage"]["complete_cact"])
        self.assertFalse(result["coverage"]["exact_cact_scope"])

    def test_job_routes_require_service_auth_and_requester_and_isolate_owner(self):
        manager = Mock()
        manager.create.return_value = {"job_id": "1" * 32, "status": "queued"}
        manager.get.side_effect = KeyError("not found")
        app = Flask(__name__)
        register_corporate_action_routes(
            app, client=object(), service_key="secret", job_manager=manager
        )
        headers = {"Authorization": "Bearer secret", "X-Requester-Id": OWNER}
        with app.test_client() as client:
            url = "/bloomberg/corporate-actions-jobs"
            self.assertEqual(client.post(url, json={}).status_code, 401)
            self.assertEqual(
                client.post(
                    url, json={}, headers={"Authorization": "Bearer secret"}
                ).status_code,
                400,
            )
            payload = {
                "operation": "start",
                "request_id": str(uuid.uuid4()),
                "request": options(),
            }
            self.assertEqual(
                client.post(url, json=payload, headers=headers).status_code, 202
            )
            manager.create.assert_called_once_with(
                OWNER, payload["request_id"], payload["request"]
            )
            self.assertEqual(
                client.post(
                    url,
                    json={"operation": "status", "job_id": "1" * 32},
                    headers=headers,
                ).status_code,
                404,
            )
            manager.get.assert_called_once_with(OWNER, "1" * 32)


class ExcelOwnershipTests(unittest.TestCase):
    def test_committed_template_is_formula_free_macro_free_and_isolated(self):
        with zipfile.ZipFile(TEMPLATE) as workbook:
            names = workbook.namelist()
            self.assertFalse(
                any("externalLink" in name or "vbaProject" in name for name in names)
            )
            for name in names:
                if name.startswith("xl/worksheets/") and name.endswith(".xml"):
                    self.assertNotIn(b"<f", workbook.read(name))
            metadata = workbook.read("xl/workbook.xml")
            self.assertIn(b'name="Query"', metadata)
            self.assertIn(b'name="Inputs"', metadata)

    def make_excel(self):
        excel = object.__new__(OwnedExcel)
        excel.book = Mock()
        excel.app = Mock()
        excel.app.Hwnd = 123
        excel.app.Workbooks.Count = 0
        excel.process = Mock(pid=55)
        excel.win32process = Mock()
        excel.win32process.GetWindowThreadProcessId.return_value = (1, 55)
        return excel

    def test_close_only_owned_workbook_and_empty_owned_process(self):
        excel = self.make_excel()
        book, app = excel.book, excel.app
        excel.close()
        book.Close.assert_called_once_with(SaveChanges=False)
        app.Quit.assert_called_once()

    def test_user_workbooks_or_other_process_are_never_quit(self):
        for other_book, other_process in [(True, False), (False, True)]:
            excel = self.make_excel()
            app = excel.app
            app.Workbooks.Count = int(other_book)
            if other_process:
                excel.win32process.GetWindowThreadProcessId.return_value = (1, 99)
            excel.close()
            app.Quit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
