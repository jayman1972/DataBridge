"""SOAR risk PDF generation, immutable saving, and service-authorized route."""

from __future__ import annotations

import io
import logging
import os
import secrets
import tempfile
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyodbc
from flask import Flask, jsonify, request, send_file

from sggg.diamond_client import get_diamond_client
from sggg.soar_risk_data import collect_soar_risk_report
from sggg.soar_risk_math import parse_iso_date, previous_friday
from sggg.soar_risk_pdf import build_soar_risk_pdf

OUTPUT_DIR = Path(
    os.environ.get(
        "SOAR_RISK_REPORTS_DIR",
        r"\\ehp04\workgroups\James Park\EHP Funds\Risk\Reports - Internal",
    )
)
TORONTO = ZoneInfo("America/Toronto")
LOGGER = logging.getLogger(__name__)


def generate_soar_risk_report(
    *, as_of: date, bbg: Any, supabase_client: Any,
) -> tuple[dict[str, Any], bytes]:
    diamond = get_diamond_client()
    if diamond is None:
        raise RuntimeError("Diamond connection is unavailable")
    connection = pyodbc.connect("DSN=PSC_VIEWER", timeout=15)
    try:
        report = collect_soar_risk_report(
            as_of=as_of, diamond=diamond, bbg=bbg,
            supabase_client=supabase_client, psc_connection=connection,
            generated_at=datetime.now(TORONTO),
        )
    finally:
        connection.close()
    return report, build_soar_risk_pdf(report)


def report_filename(as_of: date) -> str:
    return f"{as_of:%Y%m%d} The Soar Fund Risk Report.pdf"


def save_soar_risk_report(as_of: date, pdf: bytes, output_dir: Path = OUTPUT_DIR) -> Path:
    """Save once per position date; never silently replace a historical report."""
    if not pdf.startswith(b"%PDF-"):
        raise ValueError("Generated SOAR report is not a PDF")
    if not output_dir.is_dir():
        raise RuntimeError(f"Report directory is unavailable: {output_dir}")
    destination = output_dir / report_filename(as_of)
    if destination.exists():
        if destination.read_bytes() == pdf:
            return destination
        raise FileExistsError(f"A different SOAR report already exists: {destination}")
    with tempfile.NamedTemporaryFile(dir=output_dir, prefix=".soar-risk-", suffix=".pdf", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(pdf)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        if destination.exists():
            raise FileExistsError(f"SOAR report appeared while saving: {destination}")
        os.rename(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return destination


def register_soar_risk_routes(
    app: Flask, *, bbg: Any, supabase_client: Any, service_key: str,
) -> None:
    """Expose report preview/save only to the authenticated Edge Function."""

    @app.route("/sggg/risk-reports/soar", methods=["POST"])
    def soar_risk_report_route():
        supplied = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not service_key or not secrets.compare_digest(supplied, service_key):
            return jsonify({"error": "Unauthorized"}), 401
        payload = request.get_json(silent=True) or {}
        try:
            as_of = parse_iso_date(payload.get("as_of"))
            if as_of > datetime.now(TORONTO).date():
                return jsonify({"error": "Position date cannot be in the future"}), 400
            report, pdf = generate_soar_risk_report(
                as_of=as_of, bbg=bbg, supabase_client=supabase_client,
            )
            if payload.get("save_to_disk") is True:
                path = save_soar_risk_report(as_of, pdf)
                return jsonify(
                    {
                        "saved_path": str(path),
                        "filename": path.name,
                        "position_date": report["position_date"],
                        "fund_nav": report["fund_nav"],
                        "class_b_navpu": report["class_b_navpu"],
                    }
                )
            return send_file(
                io.BytesIO(pdf),
                mimetype="application/pdf",
                as_attachment=False,
                download_name=report_filename(as_of),
            )
        except (TypeError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 422
        except FileExistsError as exc:
            return jsonify({"error": str(exc)}), 409
        except Exception as exc:
            app.logger.exception("SOAR risk report failed")
            return jsonify({"error": f"SOAR risk report failed: {type(exc).__name__}"}), 500


def scheduled_as_of(now: datetime | None = None) -> date:
    current = now or datetime.now(TORONTO)
    return previous_friday(current.date())


def scheduled_report_due(now: datetime | None = None) -> date | None:
    """Retry from Monday 10 a.m. Toronto time through Friday if inputs lag."""
    current = (now or datetime.now(TORONTO)).astimezone(TORONTO)
    if current.weekday() > 4 or (current.weekday() == 0 and current.hour < 10):
        return None
    return scheduled_as_of(current)


def start_soar_risk_scheduler(*, bbg: Any, supabase_client: Any) -> threading.Thread:
    """Run in the signed-in DataBridge process that has PSC/BBG/share access."""

    def loop() -> None:
        last_error: str | None = None
        while True:
            as_of = scheduled_report_due()
            if as_of is not None:
                destination = OUTPUT_DIR / report_filename(as_of)
                if not destination.is_file():
                    try:
                        _, pdf = generate_soar_risk_report(
                            as_of=as_of, bbg=bbg, supabase_client=supabase_client,
                        )
                        saved = save_soar_risk_report(as_of, pdf)
                        print(f"[soar-risk] saved {saved}", flush=True)
                        last_error = None
                    except Exception as exc:
                        error = f"{as_of}: {type(exc).__name__}: {exc}"
                        if error != last_error:
                            LOGGER.exception("[soar-risk] weekly report pending: %s", error)
                            last_error = error
            time.sleep(300)

    worker = threading.Thread(target=loop, name="soar-risk-weekly", daemon=True)
    worker.start()
    return worker
