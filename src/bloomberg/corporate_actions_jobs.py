"""Owner-scoped, persistent on-demand jobs; no scheduler or universe backfill."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any

from .corporate_actions_bql import (
    BQL_FIELDS,
    PARTIAL_COVERAGE,
    SCOPE_COVERAGE,
    validate_job_options,
)
from .corporate_actions_excel_worker import write_json
from .corporate_actions_lookup import ACTION_FIELDS, lookup

LOGGER = logging.getLogger(__name__)
TERMINAL = {"completed", "failed", "cancelled"}
MAX_EXCEL_SECONDS = 900


class QueueFullError(RuntimeError):
    """The bounded on-demand queue is full."""


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def run_excel_job(
    job_dir: Path,
    options: dict[str, Any],
    universe: dict[str, list[str]],
    progress: Callable[[str], None],
    cancelled: Callable[[], bool],
) -> dict[str, Any]:
    write_json(job_dir / "excel-input.json", {"request": options, "universe": universe})
    environment = dict(os.environ)
    environment["PYTHONPATH"] = (
        str(Path(__file__).parents[1]) + os.pathsep + environment.get("PYTHONPATH", "")
    )
    # The Excel child needs no database/server credentials.
    for key in list(environment):
        if key.startswith(("SUPABASE_", "DATABASE_")):
            environment.pop(key)
    creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    deadline = time.monotonic() + MAX_EXCEL_SECONDS
    stopped_at = None
    previous_message = None
    with (job_dir / "excel-worker.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "bloomberg.corporate_actions_excel_worker",
                "--job-dir",
                str(job_dir),
            ],
            env=environment,
            stdout=log,
            stderr=log,
            creationflags=creation_flags,
        )
        while process.poll() is None:
            if cancelled():
                (job_dir / "cancel").touch()
                stopped_at = stopped_at or time.monotonic()
            try:
                status = read_json(job_dir / "progress.json")
                message = str(status["message"])[:240]
                if message != previous_message:
                    progress(message)
                    previous_message = message
            except (OSError, ValueError, KeyError, TypeError):
                pass  # Atomic progress file may not have been created yet.
            if time.monotonic() >= deadline or (
                stopped_at and time.monotonic() - stopped_at >= 30
            ):
                # Terminate only our Python child, never an Excel/user process.
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                try:
                    result = read_json(job_dir / "excel-partial.json")
                except (OSError, ValueError):
                    result = {"events": [], "errors": [], "warnings": []}
                result["errors"].append(
                    "Excel worker stopped before cleanup completed. Retrieved data is partial; "
                    "a job-owned Excel window may remain open. Existing user Excel processes were not terminated."
                )
                result["cancelled"] = bool(stopped_at)
                return result
            time.sleep(0.5)
    try:
        result = read_json(job_dir / "excel-result.json")
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "Excel BQL worker did not return a valid result; check its job log"
        ) from exc
    if not isinstance(result, dict) or not all(
        isinstance(result.get(key), list) for key in ("events", "errors", "warnings")
    ):
        raise RuntimeError("Excel BQL worker returned an invalid result")
    return result


def hybrid_lookup(
    client: Any,
    options: dict[str, Any],
    job_dir: Path,
    progress: Callable[[str], None],
    cancelled: Callable[[], bool],
) -> dict[str, Any]:
    progress("Resolving securities and loading Bloomberg API actions…")
    api_options = {
        **options,
        "action_types": [
            kind for kind in options["action_types"] if kind in ACTION_FIELDS
        ],
    }
    result = lookup(client, api_options, include_universe=True)
    universe = result.pop("resolved_universe")
    result["request"] = options
    result["warnings"] = [
        warning
        for warning in result["warnings"]
        if not warning.startswith("API-derived lookup,")
    ]
    result["warnings"].insert(0, PARTIAL_COVERAGE)
    if options["scope"] != "security":
        result["warnings"].append(SCOPE_COVERAGE)
    result["coverage"] = {
        "complete_cact": False,
        "exact_cact_scope": options["scope"] == "security",
        "name_changes": False,
    }
    if (
        any(kind in BQL_FIELDS for kind in options["action_types"])
        and universe
        and not cancelled()
    ):
        try:
            excel = run_excel_job(job_dir, options, universe, progress, cancelled)
            result["events"].extend(excel["events"])
            result["errors"].extend(excel["errors"])
            result["warnings"].extend(excel["warnings"])
            result["queried_count"] = max(
                result["queried_count"], len(set(excel.get("queried_securities", [])))
            )
        except (RuntimeError, OSError) as exc:
            LOGGER.exception("Excel BQL corporate-action stage failed")
            result["errors"].append(f"Excel BQL actions were not retrieved: {exc}")
    result["events"].sort(key=lambda row: (row["date"], row["security"]))
    result["errors"] = list(dict.fromkeys(result["errors"]))
    result["warnings"] = list(dict.fromkeys(result["warnings"]))
    if cancelled():
        result["warnings"].append(
            "Lookup stopped by the requester. Results cover completed queries only."
        )
    return result


class JobManager:
    def __init__(
        self,
        root: Path,
        client: Any,
        *,
        runner: Callable[..., dict[str, Any]] = hybrid_lookup,
    ):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = root / "jobs.sqlite3"
        self.client = client
        self.runner = runner
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="corporate-actions"
        )
        with closing(self.connect()) as connection, connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, owner TEXT NOT NULL, "
                "request_id TEXT NOT NULL, options TEXT NOT NULL, status TEXT NOT NULL, message TEXT NOT NULL, "
                "created REAL NOT NULL, updated REAL NOT NULL, result TEXT, error TEXT, cancel INTEGER NOT NULL DEFAULT 0, "
                "UNIQUE(owner, request_id))"
            )
            connection.execute(
                "UPDATE jobs SET status='failed', error='DataBridge restarted before this job finished. Start a new lookup.', "
                "updated=? WHERE status IN ('queued', 'running')",
                (time.time(),),
            )

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def create(self, owner: str, request_id: str, options: Any) -> dict[str, Any]:
        options = validate_job_options(options)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("A UUID request_id is required") from exc
        encoded = json.dumps(options, sort_keys=True)
        with self.lock, closing(self.connect()) as connection, connection:
            previous = connection.execute(
                "SELECT * FROM jobs WHERE owner=? AND request_id=?", (owner, request_id)
            ).fetchone()
            if previous:
                if previous["options"] != encoded:
                    raise ValueError(
                        "request_id was already used for a different lookup"
                    )
                return self.serialize(previous)
            count = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('queued','running')"
            ).fetchone()[0]
            if count >= 4:
                raise QueueFullError(
                    "Four corporate-action batches are already queued. Retry shortly."
                )
            job_id = uuid.uuid4().hex
            now = time.time()
            connection.execute(
                "INSERT INTO jobs(id,owner,request_id,options,status,message,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                (
                    job_id,
                    owner,
                    request_id,
                    encoded,
                    "queued",
                    "Queued for Bloomberg lookup…",
                    now,
                    now,
                ),
            )
        self.executor.submit(self._run, job_id, owner, options)
        return self.get(owner, job_id)

    @staticmethod
    def serialize(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "job_id": row["id"],
            "status": row["status"],
            "message": row["message"],
            "created_at": row["created"],
            "updated_at": row["updated"],
            "cancel_requested": bool(row["cancel"]),
            "result": json.loads(row["result"]) if row["result"] else None,
            "error": row["error"],
        }

    def get(self, owner: str, job_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE owner=? AND id=?", (owner, job_id)
            ).fetchone()
        if row is None:
            raise KeyError("Corporate-action job not found")
        return self.serialize(row)

    def cancel(self, owner: str, job_id: str) -> dict[str, Any]:
        with closing(self.connect()) as connection, connection:
            connection.execute(
                "UPDATE jobs SET cancel=1, message='Stopping the current lookup…', updated=?, "
                "status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END "
                "WHERE owner=? AND id=? AND status IN ('queued','running')",
                (time.time(), owner, job_id),
            )
        return self.get(owner, job_id)

    def _update(self, job_id: str, **values: Any) -> None:
        allowed = {"status", "message", "error", "result"}
        if not values or not set(values).issubset(allowed):
            raise ValueError("Invalid job update")
        columns = ", ".join(f"{key}=?" for key in values)
        with closing(self.connect()) as connection, connection:
            connection.execute(
                f"UPDATE jobs SET {columns}, updated=? WHERE id=?",
                (*values.values(), time.time(), job_id),
            )

    def _run(self, job_id: str, owner: str, options: dict[str, Any]) -> None:
        with closing(self.connect()) as connection, connection:
            transition = connection.execute(
                "UPDATE jobs SET status='running', message='Starting Bloomberg lookup…', updated=? "
                "WHERE id=? AND owner=? AND status='queued' AND cancel=0",
                (time.time(), job_id, owner),
            )
            if transition.rowcount != 1:
                return
        job_dir = self.root / job_id
        try:
            job_dir.mkdir(exist_ok=False)
            result = self.runner(
                self.client,
                options,
                job_dir,
                lambda message: self._update(job_id, message=message),
                lambda: self.get(owner, job_id)["cancel_requested"],
            )
            stopped = self.get(owner, job_id)["cancel_requested"]
            self._update(
                job_id,
                status="cancelled" if stopped else "completed",
                message="Stopped; completed queries are available"
                if stopped
                else "Lookup finished",
                result=json.dumps(result),
            )
        except Exception:
            LOGGER.exception("Corporate-action background job failed")
            self._update(
                job_id,
                status="failed",
                message="Lookup failed",
                error="Bloomberg lookup failed. Check the Bloomberg login and DataBridge job logs.",
            )

    def close(self) -> None:
        self.executor.shutdown(wait=True)
