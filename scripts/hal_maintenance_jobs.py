"""Run HAL's two maintenance jobs without a browser or exposed credentials."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT_URL = "https://aphjduxfgsrqswonmgyb.supabase.co"
ENDPOINT = PROJECT_URL + "/functions/v1/run-scheduled-maintenance"
JOBS = ("clarifi", "calculated-fields")
DEFAULT_STATE = Path(r"C:\Tasks\hal_maintenance_state.json")
CONFIG_LOCATIONS = (
    Path(r"C:\Users\jmann\projects\DataBridge\bloomberg-service.env"),
    Path(r"C:\Users\jmann\projects\market-dashboard\bloomberg-service.env"),
)
DEFAULT_OIL_CSV = Path(
    r"C:\Users\jmann\OneDrive\Desktop\EHP_Files\DailyExports from Clarifi\OilDemand.csv"
)


class JobError(Exception):
    """An actionable failure that must not leak the server credential."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_credential() -> str:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    environment_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    if key and environment_url and environment_url != PROJECT_URL:
        raise JobError("DataBridge credential targets a different Supabase project")
    if not key:
        for location in CONFIG_LOCATIONS:
            if not location.is_file():
                continue
            settings = {}
            try:
                lines = location.read_text(encoding="utf-8-sig").splitlines()
            except OSError:
                raise JobError("DataBridge credential file could not be read") from None
            for line in lines:
                if "=" in line and not line.lstrip().startswith("#"):
                    name, value = line.split("=", 1)
                    settings[name.strip()] = value.strip()
            if settings.get("SUPABASE_URL", "").rstrip("/") != PROJECT_URL:
                raise JobError(
                    "DataBridge credential targets a different Supabase project"
                )
            key = settings.get("SUPABASE_SERVICE_ROLE_KEY", "")
            break
    if not key or key.startswith("your_"):
        raise JobError("DataBridge server credential is unavailable")
    return key


class MaintenanceClient:
    def __init__(self, key: str):
        self._key = key

    def _request(self, url: str, body: dict | None = None) -> dict | list:
        headers = {"Authorization": f"Bearer {self._key}", "apikey": self._key}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(
            url,
            data=json.dumps(body).encode("utf-8") if body is not None else None,
            headers=headers,
            method="POST" if body is not None else "GET",
        )
        try:
            with urlopen(request, timeout=45) as response:
                return json.load(response)
        except HTTPError as exc:
            raise JobError(f"Authenticated request failed (HTTP {exc.code})") from None
        except (URLError, TimeoutError, OSError, ValueError):
            raise JobError(
                "Request outcome uncertain; inspect status before retrying"
            ) from None

    def action(self, action: str, **kwargs: object) -> dict:
        if action not in {"list", "startJob", "getRunDetails"}:
            raise JobError("Unsupported maintenance action")
        result = self._request(ENDPOINT, {"action": action, **kwargs})
        if not isinstance(result, dict) or result.get("error"):
            raise JobError("Maintenance endpoint returned an invalid response")
        return result

    def job(self, job_key: str) -> dict:
        if job_key not in JOBS:
            raise JobError("Unsupported maintenance job")
        jobs = self.action("list").get("jobs", [])
        job = next((item for item in jobs if item.get("jobKey") == job_key), None)
        if not isinstance(job, dict) or not job.get("enabled"):
            raise JobError(f"{job_key} is missing or disabled")
        if not job.get("manualTriggerAllowed"):
            raise JobError(f"{job_key} does not allow manual runs")
        expected = (
            ("update", "provider", "clarifi")
            if job_key == "clarifi"
            else ("client-orchestrator", "orchestrator", "calculated_incremental")
        )
        target, field, value = expected
        if (
            job.get("handlerTarget") != target
            or (job.get("handlerBody") or {}).get(field) != value
        ):
            raise JobError(
                f"{job_key} handler configuration changed; review before starting"
            )
        return job

    def latest(self, job_key: str) -> dict | None:
        return self.job(job_key).get("lastRun")

    def start(self, job_key: str) -> dict:
        if job_key not in JOBS:
            raise JobError("Unsupported maintenance job")
        result = self.action("startJob", jobKey=job_key)
        if result.get("ok") is not True or not valid_run_id(result.get("runId")):
            raise JobError("Start response did not identify a run; inspect status")
        return result

    def run(self, run_id: int, job_key: str) -> dict:
        if job_key not in JOBS or not valid_run_id(run_id):
            raise JobError("Unsupported run lookup")
        run = self.action("getRunDetails", runId=run_id).get("run")
        if not isinstance(run, dict) or run.get("job_key") != job_key:
            raise JobError("Run identity does not match the requested job")
        return run

    def oil_rows(self) -> list[dict]:
        query = urlencode(
            {
                "select": "date,oil_supply_demand_index,oil_supply_demand_indicator",
                "oil_supply_demand_index": "not.is.null",
                "order": "date.desc",
                "limit": "2",
            }
        )
        rows = self._request(PROJECT_URL + "/rest/v1/market_data?" + query)
        if not isinstance(rows, list):
            raise JobError("Oil market-data response is invalid")
        return rows


def valid_run_id(value: object) -> bool:
    return type(value) is int and value > 0


def oil_csv_observation(path: Path) -> dict:
    if not path.is_file():
        raise JobError("Verified OilDemand.csv is missing")
    raw = path.read_bytes()
    observations = {}
    try:
        lines = raw.decode("utf-8-sig").splitlines()
    except UnicodeError:
        raise JobError("OilDemand.csv is not valid UTF-8") from None
    for row in csv.reader(lines):
        if len(row) < 2:
            continue
        try:
            day = (
                datetime.strptime(row[0].strip(), "%m/%d/%Y")
                .replace(tzinfo=timezone.utc)
                .date()
            )
            value = Decimal(row[1].strip())
        except (ValueError, InvalidOperation):
            continue
        if not value.is_finite() or day in observations:
            raise JobError("OilDemand.csv contains invalid or duplicate observations")
        observations[day] = value
    if len(observations) < 2:
        raise JobError("OilDemand.csv has fewer than two valid observations")
    latest = max(observations)
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "date": latest.isoformat(),
        "value": str(observations[latest]),
        "bytes": len(raw),
    }


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            return state
    except (OSError, ValueError):
        pass
    raise JobError("State file is unreadable; do not start a duplicate run")


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=".hal_maintenance_",
        delete=False,
    ) as temporary:
        json.dump(state, temporary, separators=(",", ":"))
        temporary.flush()
        os.fsync(temporary.fileno())
        temp_name = temporary.name
    os.replace(temp_name, path)


@contextmanager
def exclusive_run(path: Path):
    lock_path = path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise JobError("Another HAL maintenance caller is active") from None
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def run_summary(run: dict, secret: str = "") -> dict:
    details = run.get("details") or {}
    results = details.get("results") or {}
    errors = []
    warnings = []

    def safe(value: object) -> str:
        message = str(value)
        return message.replace(secret, "[redacted]") if secret else message

    def messages(value: object) -> list[str]:
        if not value:
            return []
        if isinstance(value, list):
            return [safe(item) for item in value if item]
        return [safe(value)]

    for name, result in results.items():
        if not isinstance(result, dict):
            continue
        errors.extend(f"{name}: {item}" for item in messages(result.get("errors")))
        warnings.extend(f"{name}: {item}" for item in messages(result.get("warnings")))
        if name == "Clarifi Data":
            for item in result.get("debug", {}).get("files_processed", []):
                if item.get("file", "").lower() == "oildemand.csv":
                    errors.extend(
                        f"OilDemand.csv: {error}"
                        for error in messages(item.get("errors"))
                    )
    errors.extend(messages(details.get("errorMessages")))
    warnings.extend(messages(details.get("warningMessages")))
    return {
        "runId": run.get("id"),
        "jobKey": run.get("job_key"),
        "completedAt": run.get("completed_at"),
        "success": run.get("success"),
        "skipped": run.get("skipped"),
        "message": safe(run.get("message")),
        "currentStep": run.get("current_step"),
        "errorCount": details.get("errorCount", 0),
        "warningCount": details.get("warningCount", 0),
        "warningsFlag": details.get("warnings", False),
        "errors": errors,
        "warnings": warnings,
    }


def ensure_success(run: dict, job_key: str, secret: str = "") -> dict:
    summary = run_summary(run, secret)
    if not summary["completedAt"]:
        raise JobError(f"{job_key} run {summary['runId']} is not complete")
    if (
        summary["success"] is not True
        or summary["skipped"]
        or summary["errorCount"]
        or summary["warningCount"]
        or summary["warningsFlag"]
        or summary["errors"]
        or summary["warnings"]
    ):
        raise JobError(
            f"{job_key} run {summary['runId']} failed or has material warnings"
        )
    results = (run.get("details") or {}).get("results") or {}
    required = (
        ("Clarifi Data", "EHP Files") if job_key == "clarifi" else ("CalculatedFields",)
    )
    if any(name not in results for name in required):
        raise JobError(f"{job_key} run {summary['runId']} lacks required results")
    if job_key == "clarifi":
        files = results["Clarifi Data"].get("debug", {}).get("files_processed", [])
        if not any(item.get("file", "").lower() == "oildemand.csv" for item in files):
            raise JobError("Clarifi completed without processing OilDemand.csv")
        ehp_files = results["EHP Files"].get("debug", {}).get("files_processed", [])
        if not ehp_files or any(
            not item.get("found") or not item.get("size") for item in ehp_files
        ):
            raise JobError("EHP files are missing or empty in the Clarifi run")
    else:
        steps = results["CalculatedFields"].get("debug", {}).get("steps", [])
        if not any(item.get("step") == "updates_complete" for item in steps):
            raise JobError(
                "Calculated fields did not report a complete recent-window update"
            )
    return summary


def oil_transition(
    rows: list[dict], observation: dict, last_notified: str | None
) -> dict:
    if len(rows) != 2:
        raise JobError("Two fresh oil observations are required")
    latest, previous = rows
    if latest.get("date") != observation["date"]:
        raise JobError("Persisted oil date does not match the verified CSV")
    try:
        db_value = Decimal(str(latest["oil_supply_demand_index"]))
    except (InvalidOperation, KeyError):
        raise JobError("Persisted oil observation is invalid") from None
    if abs(db_value - Decimal(observation["value"])) > Decimal("0.00005"):
        raise JobError("Persisted oil value does not match the verified CSV")
    states = [row.get("oil_supply_demand_indicator") for row in rows]
    if any(type(value) is not int or value not in (0, 1) for value in states):
        raise JobError("Oil indicator must contain explicit 0/1 states")
    changed = states[0] != states[1]
    transition_id = f"{latest['date']}:{states[1]}>{states[0]}" if changed else None
    return {
        "observationDate": latest["date"],
        "previousObservationDate": previous["date"],
        "indicator": "Rising" if states[0] == 1 else "Falling",
        "previousIndicator": "Rising" if states[1] == 1 else "Falling",
        "transitionId": transition_id,
        "notify": changed and transition_id != last_notified,
    }


def resolve_run(
    client: MaintenanceClient, path: Path, state: dict, job_key: str
) -> int:
    runs = state["cycle"].setdefault("runs", {})
    slot = runs.get(job_key)
    if isinstance(slot, int):
        return slot
    if isinstance(slot, dict) and slot.get("starting"):
        latest = client.latest(job_key)
        baseline = slot.get("baseline", 0)
        if latest and valid_run_id(latest.get("id")) and latest["id"] > baseline:
            run_id = latest["id"]
            runs[job_key] = run_id
            save_state(path, state)
            return run_id
        raise JobError(
            f"{job_key} start is uncertain; inspect server history before retrying"
        )
    latest = client.latest(job_key)
    if latest and not latest.get("completedAt"):
        run_id = latest["id"]
        runs[job_key] = run_id
        save_state(path, state)
        return run_id
    baseline = latest["id"] if latest else 0
    runs[job_key] = {"starting": utc_now(), "baseline": baseline}
    save_state(path, state)
    started = client.start(job_key)
    runs[job_key] = started["runId"]
    save_state(path, state)
    return started["runId"]


def wait_run(
    client: MaintenanceClient, run_id: int, job_key: str, seconds: int, deadline: float
) -> dict:
    while True:
        run = client.run(run_id, job_key)
        if run.get("completed_at"):
            return run
        if time.monotonic() >= deadline:
            raise JobError(f"{job_key} run {run_id} is still active; resume with run")
        time.sleep(min(seconds, max(0, deadline - time.monotonic())))


def execute(
    client: MaintenanceClient, path: Path, csv_path: Path, poll: int, timeout: int
) -> dict:
    observation = oil_csv_observation(csv_path)
    with exclusive_run(path):
        state = load_state(path)
        cycle = state.get("cycle", {})
        if cycle and cycle.get("sha256") != observation["sha256"]:
            if cycle.get("phase") != "complete":
                raise JobError(
                    "Previous cycle is incomplete; do not replace its verified CSV"
                )
            cycle = {}
        if not cycle:
            cycle = {
                **observation,
                "runs": {},
                "phase": "running",
                "startedAt": utc_now(),
            }
            state["cycle"] = cycle
            save_state(path, state)
        if cycle.get("phase") == "complete":
            return cycle_result(cycle, already_complete=True)
        deadline = time.monotonic() + timeout
        summaries = {}
        for job_key in JOBS:
            run_id = resolve_run(client, path, state, job_key)
            run = wait_run(client, run_id, job_key, poll, deadline)
            summaries[job_key] = ensure_success(run, job_key, client._key)
            cycle.setdefault("summaries", {})[job_key] = summaries[job_key]
            save_state(path, state)
        signal = oil_transition(
            client.oil_rows(), observation, state.get("lastNotified")
        )
        if signal["notify"]:
            state["lastNotified"] = signal["transitionId"]
        cycle["signal"] = signal
        cycle["phase"] = "complete"
        cycle["completedAt"] = utc_now()
        save_state(path, state)
        return cycle_result(cycle, already_complete=False)


def cycle_result(cycle: dict, *, already_complete: bool) -> dict:
    signal = dict(cycle.get("signal") or {})
    if already_complete:
        signal["notify"] = False
    return {
        "phase": cycle["phase"],
        "oilDate": cycle["date"],
        "runIds": cycle["runs"],
        "runs": cycle.get("summaries", {}),
        "signal": signal,
        "notify": signal.get("notify", False),
        "alreadyComplete": already_complete,
    }


def probe(client: MaintenanceClient) -> dict:
    return {
        "project": "aphjduxfgsrqswonmgyb",
        "jobs": [
            {
                "jobKey": job_key,
                "lastRunId": (client.job(job_key).get("lastRun") or {}).get("id"),
            }
            for job_key in JOBS
        ],
    }


def status(client: MaintenanceClient, path: Path, run_id: int | None = None) -> dict:
    state = load_state(path)
    cycle = state.get("cycle", {})
    if run_id is not None:
        for job_key in JOBS:
            if cycle.get("runs", {}).get(job_key) == run_id:
                return {"run": run_summary(client.run(run_id, job_key), client._key)}
        raise JobError("Run ID is not recorded for this HAL cycle")
    runs = {}
    for job_key, entry in cycle.get("runs", {}).items():
        if isinstance(entry, int):
            runs[job_key] = run_summary(client.run(entry, job_key), client._key)
        else:
            runs[job_key] = entry
    return {
        "phase": cycle.get("phase"),
        "oilDate": cycle.get("date"),
        "runs": runs,
        "signal": cycle.get("signal"),
    }


def retry_failed(client: MaintenanceClient, path: Path, job_key: str) -> dict:
    if job_key not in JOBS:
        raise JobError("Only the two HAL maintenance jobs may be retried")
    with exclusive_run(path):
        state = load_state(path)
        cycle = state.get("cycle", {})
        if cycle.get("phase") != "running":
            raise JobError(
                "No incomplete HAL cycle is available for a failed-run retry"
            )
        runs = cycle.get("runs", {})
        if any(isinstance(slot, dict) for slot in runs.values()):
            raise JobError("An uncertain start must be reviewed before retrying")
        run_id = runs.get(job_key)
        if not valid_run_id(run_id):
            raise JobError(f"No recorded {job_key} run can be retried")
        run = client.run(run_id, job_key)
        if not run.get("completed_at"):
            raise JobError(f"{job_key} run {run_id} is still active")
        try:
            ensure_success(run, job_key, client._key)
        except JobError:
            pass
        else:
            raise JobError(f"{job_key} run {run_id} succeeded; retry is not allowed")
        affected = JOBS if job_key == "clarifi" else ("calculated-fields",)
        for affected_job in affected:
            other_id = runs.get(affected_job)
            if valid_run_id(other_id) and other_id != run_id:
                other_run = client.run(other_id, affected_job)
                if not other_run.get("completed_at"):
                    raise JobError(f"{affected_job} run {other_id} is still active")
        for affected_job in affected:
            runs.pop(affected_job, None)
            cycle.get("summaries", {}).pop(affected_job, None)
        save_state(path, state)
        return {"retryReady": job_key, "previousRunId": run_id}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "status", "probe", "retry-failed"))
    parser.add_argument("--oil-csv", type=Path, default=DEFAULT_OIL_CSV)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--job-key", choices=JOBS)
    parser.add_argument("--poll-seconds", type=int, default=20)
    parser.add_argument("--timeout-minutes", type=int, default=180)
    args = parser.parse_args()
    try:
        if args.poll_seconds < 1 or args.timeout_minutes < 1:
            raise JobError("Poll and timeout values must be positive")
        client = MaintenanceClient(load_credential())
        if args.command == "probe":
            result = probe(client)
        elif args.command == "status":
            result = status(client, args.state, args.run_id)
        elif args.command == "retry-failed":
            if not args.job_key:
                raise JobError("--job-key is required for retry-failed")
            result = retry_failed(client, args.state, args.job_key)
        else:
            if args.run_id is not None:
                raise JobError("--run-id is only valid with status")
            result = execute(
                client,
                args.state,
                args.oil_csv,
                args.poll_seconds,
                args.timeout_minutes * 60,
            )
        print(json.dumps(result, separators=(",", ":"), default=str))
        return 0
    except JobError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
