"""
Fund Admin operations CSV reports (country breakdown, gross PnL by side, ETFs).

Mirrors legacy utilities scripts:
  get_country_breakdown.py
  get_gross_pnl_by_side.py
  get_etfs.py

Incremental runs: when a cumulative CSV already exists on disk, only query PSC for
dates after the latest POSN_DATE in that file, then append and rewrite the cumulative
file so quarterly runs avoid re-scanning history from 2018.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

REPORT_COUNTRY_BREAKDOWN = "country_breakdown"
REPORT_GROSS_PNL_BY_SIDE = "gross_pnl_by_side"
REPORT_ETFS = "etfs"

OPERATIONS_REPORT_TYPES = frozenset(
    {REPORT_COUNTRY_BREAKDOWN, REPORT_GROSS_PNL_BY_SIDE, REPORT_ETFS}
)

POSN_DATE_COLUMN = "POSN_DATE"
FULL_REPORT_SUFFIX = "_full"
META_SUFFIX = ".meta.json"

# PSC portfolios used by all three legacy scripts.
OPERATIONS_PORTFOLIOS: Tuple[str, ...] = (
    "EHP Adv Alt",
    "EHP Adv Intl Alt",
    "EHP Advantage Intl Alt",
    "EHP Found Alt",
    "EHP Found Intl Alt",
    "EHP Foundation Intl Alt",
    "EHP Global Arb",
    "EHP Select Alt",
    "EHP Advantage",
    "EHP Foundation Fund",
    "EHP GMS Fund",
    "EHP GMS Alt",
    "EHP GMS Alt Hedge",
    "EHP Strat Inc Alt",
    "EHP Global ESG Alt",
    "EHP Global ESG Alt H",
    "EHP Multi Asset Fund",
    "EHP Multi Asset Hedg",
    "The Soar Fund",
    "The Soar Fund Hedge",
    "EHP Tact Growth Alt",
    "EHP TactGrowth Alt H",
    "EHP Alpha",
    "EHP Alpha Hedge",
)

# Default ETF / levered product list is managed in the Fund Admin UI (saved per user).
# The ETF report requires etf_securities from the client.
ETF_SECURITY_RE = re.compile(r"^[A-Z0-9]+(?:\.[A-Z0-9]+)*(?:\.[A-Z]{2,3})?$")

DEFAULT_OUTPUT_DIR = os.environ.get(
    "OPERATIONS_REPORTS_DIR",
    r"W:\Operations Reports",
)


def parse_etf_securities_text(text: str) -> List[str]:
    """Parse comma/newline separated SECURITY values (e.g. SPY.US, EHF100I)."""
    raw = re.split(r"[\s,]+", (text or "").strip())
    out: List[str] = []
    seen: set[str] = set()
    for token in raw:
        sym = token.strip().upper()
        if not sym or sym in seen:
            continue
        if not ETF_SECURITY_RE.match(sym):
            raise ValueError(
                f"Invalid security {sym!r}. Use TICKER.EXCHANGE (e.g. SPY.US) or internal codes like EHF100I."
            )
        seen.add(sym)
        out.append(sym)
    if not out:
        raise ValueError("At least one ETF security is required.")
    return out


def format_etf_securities_text(securities: Sequence[str]) -> str:
    return ", ".join(sorted(set(securities)))


# Known PSC SECURITY duplicates — exact extras only (no prefix/wildcard matching).
PSC_ETF_SECURITY_ALIASES: dict[str, Tuple[str, ...]] = {
    "QQQ.US": ("QQQ.US OLD",),
}


def expand_etf_securities_for_query(securities: Sequence[str]) -> List[str]:
    """Exact IN-list for PSC, plus configured alias rows when a canonical ticker is listed."""
    out: List[str] = []
    seen: set[str] = set()
    for security in securities:
        sym = security.strip().upper()
        extras = PSC_ETF_SECURITY_ALIASES.get(sym, ())
        for candidate in (sym, *extras):
            if candidate in seen:
                continue
            seen.add(candidate)
            out.append(candidate)
    return out


def _filter_csv_by_posn_date_range(
    csv_text: str, start_compact: str, end_compact: str
) -> str:
    """Slice cumulative CSV to the UI-requested POSN_DATE window."""
    reader = csv.reader(io.StringIO(csv_text))
    try:
        header = next(reader)
    except StopIteration:
        return csv_text
    try:
        date_idx = header.index(POSN_DATE_COLUMN)
    except ValueError:
        return csv_text
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(header)
    for row in reader:
        if date_idx >= len(row):
            continue
        compact = _normalize_posn_date_compact(row[date_idx])
        if compact and start_compact <= compact <= end_compact:
            writer.writerow(row)
    return buf.getvalue()


def _compact_to_period_token(compact: str) -> str:
    """YYYYMMDD -> MMDDYYYY for run filenames (e.g. 06302025)."""
    if len(compact) != 8:
        return compact
    return f"{compact[4:6]}{compact[6:8]}{compact[0:4]}"


def period_report_filename(start_compact: str, end_compact: str) -> str:
    return (
        f"{_compact_to_period_token(start_compact)}"
        f"_{_compact_to_period_token(end_compact)}.csv"
    )


def _portfolio_placeholders(n: int) -> str:
    return ",".join("?" * n)


def _fetchall_dicts(cursor) -> Tuple[List[str], List[dict]]:
    columns = [d[0] for d in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return columns, rows


def _rows_to_csv(columns: Sequence[str], rows: Sequence[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(col) for col in columns])
    return buf.getvalue()


def _save_csv_to(path: Path, csv_text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp_path.write_text(csv_text, encoding="utf-8-sig", newline="")
        os.replace(tmp_path, path)
    except PermissionError as exc:
        raise PermissionError(
            f"Cannot write {path}. The CSV may be open in Excel, or the Data Bridge "
            f"Windows service account may lack Modify access on "
            f"{path.parent}. Fund Admin login permissions are not involved — "
            f"reports are saved on the PSC server share."
        ) from exc
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
    return str(path)


def _archive_previous_latest(report_type: str, stamp: datetime) -> Optional[str]:
    """Keep one backup of the prior cumulative file before overwriting latest.csv."""
    latest_path = _latest_report_path(report_type)
    legacy_full = _full_report_path(report_type)
    source = latest_path if latest_path.is_file() else legacy_full if legacy_full.is_file() else None
    if source is None:
        return None
    archive_dir = _report_dir(report_type) / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"latest_{stamp.strftime('%Y%m%d_%H%M%S')}.csv"
    try:
        shutil.copy2(source, archive_path)
    except PermissionError:
        # Prior cumulative may be open elsewhere; still attempt to write new latest.
        return None
    return str(archive_path)


def _save_run_outputs(
    *,
    report_type: str,
    merged_csv: str,
    requested_start_compact: str,
    requested_end_compact: str,
    stamp: datetime,
) -> dict[str, Optional[str]]:
    """
    Persist:
      - latest.csv — full cumulative history (incremental runs append here)
      - runs/MMDDYYYY_MMDDYYYY.csv — UI date-range slice for this run
    """
    report_dir = _report_dir(report_type)
    runs_dir = report_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    archived_path = _archive_previous_latest(report_type, stamp)

    period_csv = _filter_csv_by_posn_date_range(
        merged_csv, requested_start_compact, requested_end_compact
    )
    period_name = period_report_filename(requested_start_compact, requested_end_compact)
    period_path = _save_csv_to(runs_dir / period_name, period_csv)

    latest_path = _latest_report_path(report_type)
    saved_path = _save_csv_to(latest_path, merged_csv)

    return {
        "saved_path": saved_path,
        "period_report_path": period_path,
        "archived_path": archived_path,
    }


def _report_dir(report_type: str) -> Path:
    return Path(DEFAULT_OUTPUT_DIR) / report_type


def _latest_report_path(report_type: str) -> Path:
    return _report_dir(report_type) / "latest.csv"


def _full_report_path(report_type: str) -> Path:
    """Legacy flat cumulative filename (pre-subfolder layout)."""
    return Path(DEFAULT_OUTPUT_DIR) / f"{report_type}{FULL_REPORT_SUFFIX}.csv"


def _full_report_meta_path(report_type: str) -> Path:
    latest_meta = _report_dir(report_type) / f"latest{META_SUFFIX}"
    if latest_meta.is_file():
        return latest_meta
    return Path(DEFAULT_OUTPUT_DIR) / f"{report_type}{FULL_REPORT_SUFFIX}{META_SUFFIX}"


def _normalize_posn_date_compact(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    digits = re.sub(r"\D", "", text)
    return digits[:8] if len(digits) >= 8 else None


def _compact_add_days(compact: str, days: int) -> str:
    dt = datetime.strptime(compact, "%Y%m%d") + timedelta(days=days)
    return dt.strftime("%Y%m%d")


def _etf_list_fingerprint(securities: Sequence[str]) -> str:
    normalized = ",".join(sorted({s.strip().upper() for s in securities if s.strip()}))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _read_text_stripping_bom(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    return raw.decode("utf-8")


def _max_posn_date_in_csv(csv_text: str) -> Optional[str]:
    reader = csv.reader(io.StringIO(csv_text))
    try:
        header = next(reader)
    except StopIteration:
        return None
    try:
        date_idx = header.index(POSN_DATE_COLUMN)
    except ValueError:
        return None
    max_compact: Optional[str] = None
    for row in reader:
        if date_idx >= len(row):
            continue
        compact = _normalize_posn_date_compact(row[date_idx])
        if compact and (max_compact is None or compact > max_compact):
            max_compact = compact
    return max_compact


def _append_csv(existing_csv: str, new_csv: str) -> str:
    existing_lines = existing_csv.splitlines()
    new_lines = new_csv.splitlines()
    if not existing_lines:
        return new_csv
    if not new_lines:
        return existing_csv
    if len(new_lines) <= 1:
        return existing_csv
    return "\r\n".join([*existing_lines, *new_lines[1:]]) + "\r\n"


def _find_existing_report_path(report_type: str) -> Optional[Path]:
    latest_path = _latest_report_path(report_type)
    if latest_path.is_file():
        return latest_path

    legacy_full = _full_report_path(report_type)
    if legacy_full.is_file():
        return legacy_full

    out_dir = Path(DEFAULT_OUTPUT_DIR)
    if not out_dir.is_dir():
        return None
    candidates = [
        p
        for p in out_dir.glob(f"{report_type}_*.csv")
        if FULL_REPORT_SUFFIX not in p.stem
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_report_meta(report_type: str) -> dict[str, Any]:
    meta_path = _full_report_meta_path(report_type)
    if not meta_path.is_file():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _write_report_meta(report_type: str, meta: dict[str, Any]) -> None:
    meta_path = _report_dir(report_type) / f"latest{META_SUFFIX}"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    # Legacy flat meta path for backward compatibility.
    legacy_meta = Path(DEFAULT_OUTPUT_DIR) / f"{report_type}{FULL_REPORT_SUFFIX}{META_SUFFIX}"
    legacy_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _save_full_report(csv_text: str, report_type: str) -> str:
    return _save_csv_to(_latest_report_path(report_type), csv_text)


def resolve_incremental_query_range(
    *,
    report_type: str,
    requested_start_compact: str,
    end_compact: str,
    incremental: bool,
    force_full_rebuild: bool,
    etf_securities: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    """
    Decide whether to query PSC for the full range or only new dates since the
    latest on-disk cumulative report.
    """
    if force_full_rebuild or not incremental:
        return {
            "query_start_compact": requested_start_compact,
            "existing_csv": None,
            "existing_path": None,
            "skipped_query": False,
            "incremental": False,
            "reason": "full_rebuild",
        }

    if report_type == REPORT_ETFS and etf_securities:
        meta = _load_report_meta(report_type)
        fp = _etf_list_fingerprint(etf_securities)
        if meta.get("etf_fingerprint") and meta.get("etf_fingerprint") != fp:
            return {
                "query_start_compact": requested_start_compact,
                "existing_csv": None,
                "existing_path": None,
                "skipped_query": False,
                "incremental": False,
                "reason": "etf_list_changed",
            }

    existing_path = _find_existing_report_path(report_type)
    if existing_path is None:
        return {
            "query_start_compact": requested_start_compact,
            "existing_csv": None,
            "existing_path": None,
            "skipped_query": False,
            "incremental": False,
            "reason": "no_existing_file",
        }

    existing_csv = _read_text_stripping_bom(existing_path)
    max_date = _max_posn_date_in_csv(existing_csv)
    if not max_date:
        return {
            "query_start_compact": requested_start_compact,
            "existing_csv": existing_csv,
            "existing_path": str(existing_path),
            "skipped_query": False,
            "incremental": False,
            "reason": "existing_file_unreadable",
        }

    if max_date >= end_compact:
        return {
            "query_start_compact": requested_start_compact,
            "existing_csv": existing_csv,
            "existing_path": str(existing_path),
            "skipped_query": True,
            "incremental": True,
            "reason": "already_current",
            "existing_max_date": max_date,
        }

    query_start = max(requested_start_compact, _compact_add_days(max_date, 1))
    return {
        "query_start_compact": query_start,
        "existing_csv": existing_csv,
        "existing_path": str(existing_path),
        "skipped_query": False,
        "incremental": True,
        "reason": "append_new_dates",
        "existing_max_date": max_date,
    }


def _country_breakdown_sql() -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    return f"""
        SELECT
            POSN_DATE,
            PORTFOLIO,
            SECURITY_TYPE,
            COUNTRY,
            LONG_SHORT,
            SUM(VALUE) AS VALUE,
            SUM(REALIZED_PROFIT) as REALIZED,
            SUM(UNREALIZED_PROFIT) as UNREALIZED,
            SUM(INTEREST) as INTEREST,
            SUM(ACCR_INT) as ACCR_INT,
            SUM(DIVIDENDS) as DIVIDENDS,
            SUM(FEES) as FEES,
            SUM(DAY_PROFIT) as DAY_PROFIT
        FROM psc_position_history
        WHERE POSN_DATE >= ? AND POSN_DATE <= ?
        AND PORTFOLIO in ({portfolio_ph})
        GROUP BY
            POSN_DATE, PORTFOLIO, SECURITY_TYPE, COUNTRY, LONG_SHORT
        ORDER BY
            POSN_DATE, PORTFOLIO, SECURITY_TYPE, COUNTRY, LONG_SHORT
    """


def _gross_pnl_by_side_sql() -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    return f"""
        SELECT
            POSN_DATE,
            STRATEGY,
            PORTFOLIO,
            SUM((VALUE>=0)*VALUE) AS `Long Market Value`,
            SUM((VALUE<0)*VALUE) AS `Short Market Value`,
            SUM((VALUE>=0)*DAY_PROFIT) AS `Long PnL`,
            SUM((VALUE<0)*DAY_PROFIT) AS `Short PnL`,
            SUM((VALUE>=0)*EXPOSURE)/PORTFOLIO_NAV AS `Long Exposure`,
            SUM((VALUE<0)*EXPOSURE)/PORTFOLIO_NAV AS `Short Exposure`,
            SUM(EXPOSURE)/PORTFOLIO_NAV AS `Net Exposure`,
            (SUM((VALUE>=0)*EXPOSURE)-SUM((VALUE<0)*EXPOSURE))/PORTFOLIO_NAV AS `Gross Exposure`,
            PORTFOLIO_NAV
        FROM psc_position_history
        WHERE POSN_DATE >= ? AND POSN_DATE <= ?
        AND PORTFOLIO in ({portfolio_ph})
        GROUP BY POSN_DATE, PORTFOLIO, STRATEGY
        ORDER BY POSN_DATE, PORTFOLIO, STRATEGY
    """


def _etfs_sql(etf_count: int) -> str:
    portfolio_ph = _portfolio_placeholders(len(OPERATIONS_PORTFOLIOS))
    etf_ph = _portfolio_placeholders(etf_count)
    return f"""
        SELECT POSN_DATE, SECURITY, DESCRIPTION, QUANTITY,
               VALUE / FX_SETTLE_TO_BASE as LOCAL_VALUE, VALUE, PORTFOLIO, ACCOUNT
        FROM psc_position_history
        WHERE PORTFOLIO in ({portfolio_ph})
        AND POSN_DATE >= ? AND POSN_DATE <= ?
        AND SECURITY in ({etf_ph})
        ORDER BY SECURITY, PORTFOLIO, POSN_DATE
    """


def _latest_period_report_path(report_type: str) -> Optional[Path]:
    meta = _load_report_meta(report_type)
    period = meta.get("period_report_path")
    if period:
        path = Path(str(period))
        if path.is_file():
            return path
    runs_dir = _report_dir(report_type) / "runs"
    if not runs_dir.is_dir():
        return None
    candidates = [
        p
        for p in runs_dir.glob("*.csv")
        if re.fullmatch(r"\d{8}_\d{8}\.csv", p.name)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def get_latest_saved_report(report_type: str) -> Optional[dict[str, Any]]:
    """Metadata for the newest cumulative operations report on disk."""
    if report_type not in OPERATIONS_REPORT_TYPES:
        raise ValueError(f"Unknown report_type: {report_type}")
    path = _find_existing_report_path(report_type)
    if path is None or not path.is_file():
        return None
    meta = _load_report_meta(report_type)
    stat = path.stat()
    updated_at = meta.get("updated_at")
    if not updated_at:
        updated_at = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    row_count = meta.get("row_count")
    if row_count is None:
        csv_text = _read_text_stripping_bom(path)
        row_count = max(0, len(csv_text.splitlines()) - 1)
    download_name = path.name
    if path.name == "latest.csv" or path == _latest_report_path(report_type):
        end_compact = meta.get("end_date")
        if end_compact and len(str(end_compact)) == 8:
            download_name = f"{report_type}_{end_compact}.csv"
    elif path == _full_report_path(report_type):
        end_compact = meta.get("end_date")
        if end_compact and len(str(end_compact)) == 8:
            download_name = f"{report_type}_{end_compact}.csv"
    period_path = _latest_period_report_path(report_type)
    period_info: Optional[dict[str, Any]] = None
    if period_path is not None:
        period_stat = period_path.stat()
        period_text = _read_text_stripping_bom(period_path)
        period_info = {
            "saved_path": str(period_path),
            "filename": period_path.name,
            "row_count": max(0, len(period_text.splitlines()) - 1),
            "file_size_bytes": period_stat.st_size,
            "start_date": meta.get("period_start_date") or meta.get("start_date"),
            "end_date": meta.get("period_end_date") or meta.get("end_date"),
        }
    return {
        "report_type": report_type,
        "filename": download_name,
        "saved_path": str(path),
        "row_count": row_count,
        "updated_at": updated_at,
        "start_date": meta.get("start_date"),
        "end_date": meta.get("end_date"),
        "file_size_bytes": stat.st_size,
        "period_report": period_info,
    }


def run_operations_report(
    cursor,
    report_type: str,
    start_compact: str,
    end_compact: str,
    etf_securities: Optional[Sequence[str]] = None,
    *,
    save_to_disk: bool = True,
    incremental: bool = True,
    force_full_rebuild: bool = False,
) -> dict[str, Any]:
    if report_type not in OPERATIONS_REPORT_TYPES:
        raise ValueError(f"Unknown report_type: {report_type}")

    plan = resolve_incremental_query_range(
        report_type=report_type,
        requested_start_compact=start_compact,
        end_compact=end_compact,
        incremental=incremental,
        force_full_rebuild=force_full_rebuild,
        etf_securities=etf_securities,
    )
    query_start_compact = plan["query_start_compact"]
    existing_csv: Optional[str] = plan.get("existing_csv")
    rows_queried = 0
    columns: List[str] = []
    merged_csv: str
    new_chunk_csv: Optional[str] = None

    if plan.get("skipped_query") and existing_csv:
        merged_csv = existing_csv
        rows_queried = 0
    elif query_start_compact > end_compact:
        merged_csv = existing_csv or ""
        rows_queried = 0
    else:
        params_base: List[Any] = [query_start_compact, end_compact, *OPERATIONS_PORTFOLIOS]

        if report_type == REPORT_COUNTRY_BREAKDOWN:
            cursor.execute(_country_breakdown_sql(), params_base)
        elif report_type == REPORT_GROSS_PNL_BY_SIDE:
            cursor.execute(_gross_pnl_by_side_sql(), params_base)
        else:
            if not etf_securities:
                raise ValueError("etf_securities required for ETF report")
            securities = list(etf_securities)
            query_securities = expand_etf_securities_for_query(securities)
            cursor.execute(
                _etfs_sql(len(query_securities)),
                [*OPERATIONS_PORTFOLIOS, query_start_compact, end_compact, *query_securities],
            )

        columns, rows = _fetchall_dicts(cursor)
        rows_queried = len(rows)
        new_chunk_csv = _rows_to_csv(columns, rows)
        merged_csv = _append_csv(existing_csv, new_chunk_csv) if existing_csv else new_chunk_csv

    row_count = max(0, len(merged_csv.splitlines()) - 1) if merged_csv else 0
    saved_path: Optional[str] = None
    period_report_path: Optional[str] = None
    archived_path: Optional[str] = None
    period_row_count = 0
    stamp = datetime.now()
    download_name = f"{report_type}_{stamp.strftime('%Y%m%d_%H%M%S')}.csv"

    if save_to_disk and merged_csv:
        outputs = _save_run_outputs(
            report_type=report_type,
            merged_csv=merged_csv,
            requested_start_compact=start_compact,
            requested_end_compact=end_compact,
            stamp=stamp,
        )
        saved_path = outputs["saved_path"]
        period_report_path = outputs.get("period_report_path")
        archived_path = outputs.get("archived_path")
        if period_report_path:
            period_text = _read_text_stripping_bom(Path(period_report_path))
            period_row_count = max(0, len(period_text.splitlines()) - 1)
        meta: dict[str, Any] = {
            "report_type": report_type,
            "updated_at": stamp.isoformat(timespec="seconds"),
            "start_date": start_compact,
            "end_date": end_compact,
            "row_count": row_count,
            "incremental": bool(plan.get("incremental")),
            "last_query_start": query_start_compact,
            "last_query_end": end_compact,
            "last_rows_queried": rows_queried,
            "period_report_path": period_report_path,
            "period_start_date": start_compact,
            "period_end_date": end_compact,
            "period_row_count": period_row_count,
            "archived_path": archived_path,
        }
        if report_type == REPORT_ETFS and etf_securities:
            meta["etf_fingerprint"] = _etf_list_fingerprint(etf_securities)
            meta["etf_count"] = len(etf_securities)
            meta["etf_query_securities"] = expand_etf_securities_for_query(list(etf_securities))
        if plan.get("existing_max_date"):
            meta["previous_max_date"] = plan["existing_max_date"]
        _write_report_meta(report_type, meta)

    return {
        "report_type": report_type,
        "start_date": start_compact,
        "end_date": end_compact,
        "row_count": row_count,
        "columns": columns,
        "csv": merged_csv,
        "filename": download_name,
        "saved_path": saved_path,
        "period_report_path": period_report_path,
        "period_row_count": period_row_count,
        "archived_path": archived_path,
        "incremental": bool(plan.get("incremental")),
        "incremental_reason": plan.get("reason"),
        "query_start_date": query_start_compact,
        "rows_queried": rows_queried,
        "existing_report_path": plan.get("existing_path"),
    }
