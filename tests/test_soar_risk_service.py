"""Route, schedule, and immutable-save checks for the SOAR risk report."""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.soar_risk_service import (
    register_soar_risk_routes,
    report_filename,
    save_soar_risk_report,
    scheduled_report_due,
)

TORONTO = ZoneInfo("America/Toronto")
REPORT_DATE = date(2026, 10, 23)


class SoarRiskServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        register_soar_risk_routes(app, bbg=object(), supabase_client=object(), service_key="test-key")
        self.client = app.test_client()

    def test_route_rejects_unauthenticated_requests(self) -> None:
        response = self.client.post(
            "/sggg/risk-reports/soar", json={"as_of": REPORT_DATE.isoformat()}
        )
        self.assertEqual(response.status_code, 401)

    def test_route_returns_pdf_with_service_key(self) -> None:
        with patch("sggg.soar_risk_service.generate_soar_risk_report") as generate:
            generate.return_value = ({"position_date": REPORT_DATE.isoformat()}, b"%PDF-test")
            response = self.client.post(
                "/sggg/risk-reports/soar",
                headers={"Authorization": "Bearer test-key"},
                json={"as_of": REPORT_DATE.isoformat()},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertEqual(response.data, b"%PDF-test")

    def test_route_rejects_invalid_date_before_generation(self) -> None:
        with patch("sggg.soar_risk_service.generate_soar_risk_report") as generate:
            response = self.client.post(
                "/sggg/risk-reports/soar",
                headers={"Authorization": "Bearer test-key"},
                json={"as_of": "2026-02-30"},
            )
            generate.assert_not_called()
        self.assertEqual(response.status_code, 422)

    def test_save_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as folder:
            output_dir = Path(folder)
            saved = save_soar_risk_report(REPORT_DATE, b"%PDF-first", output_dir)
            self.assertEqual(saved.name, report_filename(REPORT_DATE))
            self.assertEqual(saved.read_bytes(), b"%PDF-first")
            self.assertEqual(
                save_soar_risk_report(REPORT_DATE, b"%PDF-first", output_dir), saved
            )
            with self.assertRaises(FileExistsError):
                save_soar_risk_report(REPORT_DATE, b"%PDF-different", output_dir)
            self.assertEqual(saved.read_bytes(), b"%PDF-first")

    def test_monday_morning_and_late_source_catchup(self) -> None:
        self.assertIsNone(scheduled_report_due(datetime(2026, 10, 26, 9, tzinfo=TORONTO)))
        self.assertEqual(
            scheduled_report_due(datetime(2026, 10, 26, 10, tzinfo=TORONTO)), REPORT_DATE
        )
        self.assertEqual(
            scheduled_report_due(datetime(2026, 10, 28, 14, tzinfo=TORONTO)), REPORT_DATE
        )
        self.assertIsNone(scheduled_report_due(datetime(2026, 10, 31, 10, tzinfo=TORONTO)))


if __name__ == "__main__":
    unittest.main()
