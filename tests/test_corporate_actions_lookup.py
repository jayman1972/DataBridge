from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask

from bloomberg.corporate_actions_lookup import (
    fetch_batch,
    lookup,
    normalize_identifier,
    validate_request,
)
from bloomberg.corporate_actions_routes import (
    cached_lookup,
    register_corporate_action_routes,
)


def options(**changes):
    return validate_request(
        {
            "securities": ["AAPL US Equity"],
            "scope": "security",
            "date_basis": "announced",
            "action_types": ["dividends", "splits"],
            "start_date": "2025-10-20",
            "end_date": "2027-10-20",
            **changes,
        }
    )


class LookupTests(unittest.TestCase):
    def test_batch_timeout_is_reported_and_session_is_closed(self):
        session = Mock()
        session.nextEvent.return_value.eventType.return_value = 0
        client = Mock()
        client._create_session.return_value = session
        with patch(
            "bloomberg.corporate_actions_lookup.blpapi",
            SimpleNamespace(Event=SimpleNamespace(RESPONSE=1, PARTIAL_RESPONSE=2)),
        ):
            result, errors = fetch_batch(
                client,
                ["AAPL US Equity"],
                ["DVD_HIST_ALL"],
                deadline=time.monotonic() + 0.01,
            )
        self.assertEqual(result, {"AAPL US Equity": {}})
        self.assertIn("timed out", errors[0])
        session.stop.assert_called_once()

    def test_event_with_no_effective_date_is_warned_not_relabelled(self):
        def fetch(_client, _securities, fields, overrides=None, **_kwargs):
            if "ID_BB_GLOBAL" in fields:
                return {"AAPL US Equity": {"ID_BB_GLOBAL": "id"}}, []
            return {
                "AAPL US Equity": {
                    "STOCK_BUYBACK_HISTORY": [
                        {
                            "Buyback Date": "2026-01-01",
                            "Buyback Expiration Date": "2027-01-01",
                        }
                    ]
                }
            }, []

        with patch("bloomberg.corporate_actions_lookup.fetch_batch", side_effect=fetch):
            result = lookup(
                object(), options(date_basis="effective", action_types=["buybacks"])
            )
        self.assertEqual(result["events"], [])
        self.assertTrue(any("buybacks: 1" in warning for warning in result["warnings"]))

    def test_identifiers(self):
        for raw, expected in [
            ("US0378331005", "/isin/US0378331005"),
            ("037833100", "/cusip/037833100"),
            ("2046251", "/sedol1/2046251"),
            ("ISIN: US0378331005", "/isin/US0378331005"),
            ("AAPL US", "AAPL US Equity"),
            ("IBM", "IBM"),
        ]:
            self.assertEqual(normalize_identifier(raw), expected)

    def test_validation(self):
        for changes in [
            {"securities": []},
            {"securities": ["x"] * 11},
            {"scope": "all"},
            {"date_basis": "payable"},
            {"action_types": ["made_up"]},
            {"start_date": "2028-01-01"},
            {"start_date": "2020-01-01"},
        ]:
            with self.assertRaises(ValueError):
                options(**changes)
        self.assertEqual(
            len(options(securities=["AAPL US", "AAPL US Equity"])["securities"]), 1
        )

    def test_announcement_filter_does_not_apply_ex_date_override_and_dedupes_splits(
        self,
    ):
        def fetch(_client, _securities, fields, overrides=None, **_kwargs):
            if "ID_BB_GLOBAL" in fields:
                return {
                    "AAPL US Equity": {
                        "ID_BB_GLOBAL": "id",
                        "PARSEKYABLE_DES": "AAPL US Equity",
                    }
                }, []
            split = {
                "Declared Date": "2026-01-01",
                "Ex-Date": "2028-01-01",
                "Dividend Type": "Stock Split",
            }
            self.assertIsNone(overrides)
            return {
                "AAPL US Equity": {
                    "DVD_HIST_ALL": [split],
                    "EQY_DVD_HIST_SPLITS": [split],
                }
            }, []

        with patch("bloomberg.corporate_actions_lookup.fetch_batch", side_effect=fetch):
            result = lookup(object(), options())
        self.assertEqual(len(result["events"]), 1)
        self.assertEqual(result["events"][0]["action_type"], "splits")
        self.assertEqual(result["events"][0]["date"], "2026-01-01")

    def test_effective_merger_uses_action_date_not_announcement(self):
        def fetch(_client, securities, fields, overrides=None, **_kwargs):
            if "ID_BB_GLOBAL" in fields:
                return {"AAPL US Equity": {"ID_BB_GLOBAL": "id"}}, []
            if securities == ["123 Action"]:
                return {
                    "123 Action": {
                        "CA_MA_COMPLETE_DT": "2026-02-01",
                        "CA_MA_DEAL_STATUS": "Completed",
                    }
                }, []
            return {
                "AAPL US Equity": {
                    "MERGERS_AND_ACQUISITIONS": [
                        {"Action Id": 123.0, "Announcement Date": "2020-01-01"}
                    ]
                }
            }, []

        with patch("bloomberg.corporate_actions_lookup.fetch_batch", side_effect=fetch):
            result = lookup(
                object(), options(date_basis="effective", action_types=["mergers"])
            )
        self.assertEqual(result["events"][0]["date"], "2026-02-01")
        self.assertFalse(result["events"][0]["estimated"])

    def test_security_plus_expands_related_and_issuer(self):
        calls = []

        def fetch(_client, securities, fields, overrides=None, **_kwargs):
            calls.append(securities)
            if "ID_BB_GLOBAL" in fields:
                return {
                    "AAPL US Equity": {
                        "ID_BB_GLOBAL": "id",
                        "ISSUER_PARENT_EQY_TICKER": "PARENT US",
                        "RELATED_EQUITIES": [{"Ticker and Exchange Code": "OTHER US"}],
                    }
                }, []
            return {security: {} for security in securities}, []

        with patch("bloomberg.corporate_actions_lookup.fetch_batch", side_effect=fetch):
            result = lookup(object(), options(scope="security_plus"))
        self.assertEqual(result["expanded_count"], 3)
        self.assertEqual(
            set(calls[1]), {"AAPL US Equity", "PARENT US Equity", "OTHER US Equity"}
        )

    def test_missing_dates_and_security_failures_are_not_empty_success(self):
        with patch(
            "bloomberg.corporate_actions_lookup.fetch_batch",
            return_value=({}, ["provider rejected"]),
        ):
            result = lookup(object(), options())
        self.assertIn("provider rejected", result["errors"])
        self.assertTrue(any("identity" in error for error in result["errors"]))

    def test_cache_persists_and_refresh_bypasses(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch(
                "bloomberg.corporate_actions_routes.lookup",
                return_value={"events": [], "errors": []},
            ) as fetch,
        ):
            path = Path(folder) / "cache.db"
            self.assertFalse(cached_lookup(object(), options(), path)["cached"])
            self.assertTrue(cached_lookup(object(), options(), path)["cached"])
            self.assertFalse(
                cached_lookup(object(), options(), path, refresh=True)["cached"]
            )
            self.assertEqual(fetch.call_count, 2)

    def test_route_requires_service_auth_and_validates_before_lookup(self):
        app = Flask(__name__)
        register_corporate_action_routes(app, client=object(), service_key="test-key")
        with app.test_client() as client:
            self.assertEqual(
                client.post(
                    "/bloomberg/corporate-actions-lookup", json=options()
                ).status_code,
                401,
            )
            self.assertEqual(
                client.post(
                    "/bloomberg/corporate-actions-lookup",
                    json={},
                    headers={"Authorization": "Bearer test-key"},
                ).status_code,
                400,
            )


if __name__ == "__main__":
    unittest.main()
