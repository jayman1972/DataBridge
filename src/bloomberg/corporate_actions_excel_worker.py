"""A bounded child process owning one isolated Excel/Bloomberg workbook."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .corporate_actions_bql import (
    BQL_FIELDS,
    MAX_QUERY_SECURITIES,
    MAX_TABLE_ROWS,
    PendingBql,
    build_formula,
    json_value,
    parse_table,
    validate_bql_symbols,
    validate_job_options,
)

TEMPLATE = Path(__file__).parent / "templates" / "corporate_actions_bql.xlsx"
LOGGER = logging.getLogger(__name__)


class JobCancelled(RuntimeError):
    """The requesting user stopped this job."""


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(".pending")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def check_cancelled(job_dir: Path) -> None:
    if (job_dir / "cancel").exists():
        raise JobCancelled("Stopped by the requesting user")


def excel_executable() -> str:
    if os.name != "nt":
        raise RuntimeError("Excel BQL requires the signed-in Bloomberg Windows session")
    winreg = importlib.import_module("winreg")
    key_name = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\excel.exe"
    for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
        try:
            with winreg.OpenKey(hive, key_name) as key:
                value = str(winreg.QueryValue(key, None)).strip('"')
                if Path(value).is_file():
                    return value
        except OSError:
            continue
    raise RuntimeError("Microsoft Excel is not registered on this PC")


@contextmanager
def excel_job_locks(job_dir: Path, timeout: int = 120):
    """Serialize app jobs and honor the existing BEST DPS job's named lock."""
    win32event = importlib.import_module("win32event")
    win32api = importlib.import_module("win32api")
    handles = []
    acquired = []
    deadline = time.monotonic() + timeout
    try:
        for name in ("EHP_BQL_CORPORATE_ACTIONS", "EHP_BQL_BEST_DPS_BACKFILL"):
            handle = win32event.CreateMutex(None, False, name)
            handles.append(handle)
            while True:
                check_cancelled(job_dir)
                state = win32event.WaitForSingleObject(handle, 500)
                if state in (win32event.WAIT_OBJECT_0, win32event.WAIT_ABANDONED):
                    acquired.append(handle)
                    break
                if state != win32event.WAIT_TIMEOUT:
                    raise RuntimeError("Could not acquire the Excel job lock")
                write_json(
                    job_dir / "progress.json",
                    {"message": "Waiting for another Excel BQL job…"},
                )
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "Another Excel BQL job is still running. Retry later."
                    )
        yield
    finally:
        for handle in reversed(acquired):
            win32event.ReleaseMutex(handle)
        for handle in handles:
            win32api.CloseHandle(handle)


class OwnedExcel:
    """Never attaches by workbook name, changes app settings, or quits user Excel."""

    def __init__(self, workbook: Path, job_dir: Path):
        self.book = None
        self.app = None
        self.process = None
        self.job_dir = job_dir
        self.win32 = importlib.import_module("win32com.client")
        self.pythoncom = importlib.import_module("pythoncom")
        self.com_error = importlib.import_module("pywintypes").com_error
        self.win32process = importlib.import_module("win32process")
        self.workbook = workbook.resolve()

    def open(self) -> None:
        startup = subprocess.STARTUPINFO()
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = 7  # SW_SHOWMINNOACTIVE; add-in remains interactive.
        # /x starts a distinct Excel process; /r keeps the disk template copy read-only.
        # Do not use /automation or hidden DispatchEx: Bloomberg may not initialize.
        self.process = subprocess.Popen(
            [excel_executable(), "/x", "/r", str(self.workbook)],
            startupinfo=startup,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 75
        last_error = ""
        while time.monotonic() < deadline:
            check_cancelled(self.job_dir)
            try:
                # Bind only objects already registered by Excel. GetObject(path)
                # can *open* the file in a pre-existing user instance while the
                # new process is still starting, so it is deliberately forbidden.
                running = self.pythoncom.GetRunningObjectTable()
                context = self.pythoncom.CreateBindCtx(0)
                for moniker in running.EnumRunning():
                    display_name = moniker.GetDisplayName(context, None)
                    if str(display_name).casefold() != str(self.workbook).casefold():
                        continue
                    dispatch = running.GetObject(moniker).QueryInterface(
                        self.pythoncom.IID_IDispatch
                    )
                    book = self.win32.Dispatch(dispatch)
                    if Path(str(book.FullName)).resolve() != self.workbook:
                        continue
                    app = book.Application
                    pid = self.win32process.GetWindowThreadProcessId(int(app.Hwnd))[1]
                    if pid != self.process.pid:
                        continue
                    self.book, self.app = book, app
                    self.book.Windows(1).WindowState = -4140
                    return
            except self.com_error as exc:
                last_error = str(exc)
            time.sleep(1)
        raise TimeoutError(
            f"Could not attach to the isolated Excel workbook: {last_error[:160]}"
        )

    def query(self, symbols: list[str], kind: str, options: dict[str, Any]) -> Any:
        validate_bql_symbols(symbols)
        inputs = self.book.Worksheets("Inputs")
        query = self.book.Worksheets("Query")
        # Work only on this job's workbook. No RefreshAll/CalculateFull or changes
        # to application-wide calculation, alerts, events, or security settings.
        query.Cells.ClearContents()
        inputs.Range("A2:A51").ClearContents()
        cells = inputs.Range(f"A2:A{len(symbols) + 1}")
        cells.NumberFormat = "@"
        cells.Value2 = tuple((symbol,) for symbol in symbols)
        query.Range("A1").Formula = build_formula(kind, options, len(symbols))
        query.Calculate()
        deadline = time.monotonic() + 90
        started = time.monotonic()
        previous = None
        stable_reads = 0
        last_error = ""
        while time.monotonic() < deadline:
            check_cancelled(self.job_dir)
            try:
                used = query.UsedRange
                if used.Rows.Count > MAX_TABLE_ROWS or used.Columns.Count > 50:
                    raise ValueError("Excel BQL result exceeds the bounded table size")
                values = used.Value2
                if not isinstance(values, (tuple, list)):
                    values = ((values,),)
                if values == previous:
                    stable_reads += 1
                else:
                    previous, stable_reads = values, 0
                # A table is complete only after repeated stable reads and a
                # minimum add-in settle period, not merely a non-empty first cell.
                if stable_reads >= 3 and time.monotonic() - started >= 5:
                    write_json(
                        self.job_dir / f"{kind}-latest.json",
                        [[json_value(value) for value in row] for row in values],
                    )
                    parse_table(
                        values, kind, options, {symbol: [symbol] for symbol in symbols}
                    )
                    return values
            except PendingBql as exc:
                last_error = str(exc)
            except ValueError as exc:
                last_error = str(exc)
                if stable_reads >= 5 and time.monotonic() - started >= 10:
                    raise ValueError(
                        f"{last_error}; cell A1: {str(query.Range('A1').Text)[:300]}"
                    ) from exc
            except self.com_error as exc:
                if not exc.args or exc.args[0] != -2147418111:
                    raise
                last_error = "Excel is busy"
            time.sleep(1)
        raise TimeoutError(f"Bloomberg Excel query did not finish: {last_error}")

    def close(self) -> None:
        if (
            self.process is not None
            and self.app is None
            and self.process.poll() is None
        ):
            raise RuntimeError(
                "The unattached job-owned Excel session may still be open"
            )
        if self.book is not None:
            self.book.Close(SaveChanges=False)
            self.book = None
        if self.app is not None and self.process is not None:
            pid = self.win32process.GetWindowThreadProcessId(int(self.app.Hwnd))[1]
            if pid == self.process.pid and self.app.Workbooks.Count == 0:
                self.app.Quit()
            self.app = None


def run(job_dir: Path) -> dict[str, Any]:
    payload = json.loads((job_dir / "excel-input.json").read_text(encoding="utf-8"))
    options = validate_job_options(payload["request"])
    universe = payload["universe"]
    if not isinstance(universe, dict) or len(universe) > 500:
        raise ValueError("Invalid Excel security universe")
    symbols = list(universe)
    for start in range(0, len(symbols), MAX_QUERY_SECURITIES):
        validate_bql_symbols(symbols[start : start + MAX_QUERY_SECURITIES])
    kinds = [kind for kind in options["action_types"] if kind in BQL_FIELDS]
    total = len(kinds) * (
        (len(symbols) + MAX_QUERY_SECURITIES - 1) // MAX_QUERY_SECURITIES
    )
    result = {
        "events": [],
        "errors": [],
        "warnings": [],
        "queried_securities": [],
        "completed_queries": 0,
        "total_queries": total,
    }
    if not total:
        return result
    workbook = job_dir / "corporate-actions.xlsx"
    shutil.copyfile(TEMPLATE, workbook)
    pythoncom = importlib.import_module("pythoncom")
    pythoncom.CoInitialize()
    excel = OwnedExcel(workbook, job_dir)
    try:
        with excel_job_locks(job_dir):
            write_json(
                job_dir / "progress.json",
                {"message": "Opening isolated Excel/Bloomberg workbook…"},
            )
            excel.open()
            for kind in kinds:
                for start in range(0, len(symbols), MAX_QUERY_SECURITIES):
                    check_cancelled(job_dir)
                    chunk = symbols[start : start + MAX_QUERY_SECURITIES]
                    result["queried_securities"] = sorted(
                        set(result["queried_securities"]) | set(chunk)
                    )
                    message = f"Excel BQL: {kind.replace('_', ' ')} · query {result['completed_queries'] + 1}/{total}"
                    write_json(job_dir / "progress.json", {"message": message})
                    try:
                        values = excel.query(chunk, kind, options)
                        events, warnings = parse_table(
                            values,
                            kind,
                            options,
                            {symbol: universe[symbol] for symbol in chunk},
                        )
                        result["events"].extend(events)
                        result["warnings"].extend(warnings)
                        # Retain the raw returned table in this owned job folder.
                        write_json(
                            job_dir / f"{kind}-{start}.json",
                            [[json_value(value) for value in row] for row in values],
                        )
                    except (ValueError, TimeoutError) as exc:
                        result["errors"].append(
                            f"Excel BQL {kind} for securities {start + 1}–{start + len(chunk)}: {exc}"
                        )
                    result["completed_queries"] += 1
                    write_json(job_dir / "excel-partial.json", result)
    except JobCancelled:
        result["cancelled"] = True
    finally:
        try:
            excel.close()
        except Exception as exc:
            LOGGER.exception("Could not close job-owned Excel session")
            result["errors"].append(
                f"Could not close the job-owned Excel workbook cleanly: {type(exc).__name__}"
            )
        pythoncom.CoUninitialize()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-dir", type=Path, required=True)
    args = parser.parse_args()
    job_dir = args.job_dir.resolve(strict=True)
    try:
        result = run(job_dir)
        write_json(job_dir / "excel-result.json", result)
        return 0
    except Exception as exc:
        LOGGER.exception("Excel BQL worker failed")
        write_json(
            job_dir / "excel-result.json",
            {
                "events": [],
                "warnings": [],
                "errors": [
                    f"Excel BQL worker failed: {type(exc).__name__}: {str(exc)[:300]}"
                ],
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
