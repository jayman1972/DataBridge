import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask

from bloomberg.intraday import fetch_intraday_bars, register_intraday_routes


class Bars:
    def __init__(self, points):
        self.points = points

    def getElement(self, _name):
        return self

    def numValues(self):
        return len(self.points)

    def getValueAsElement(self, index):
        stamp, close = self.points[index]
        return SimpleNamespace(
            getElementAsDatetime=lambda _name: stamp,
            getElementAsFloat=lambda _name: close,
        )


def response(points, event_type=2, error=None):
    message = SimpleNamespace(
        hasElement=lambda name: name == ("responseError" if error else "barData"),
        getElement=lambda _name: error if error else Bars(points),
        messageType=lambda: "IntradayBarResponse",
    )

    class Event(list):
        def eventType(self):
            return event_type

    return Event([message])


class BloombergIntradayTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 26, 15, tzinfo=UTC)
        self.query = Mock()
        self.session = Mock()
        self.session.getService.return_value.createRequest.return_value = self.query
        self.client = Mock()
        self.client.open_refdata_session.return_value = self.session
        self.api = SimpleNamespace(Event=SimpleNamespace(RESPONSE=2))

    def fetch(self, **kwargs):
        return fetch_intraday_bars(self.client, "ESA Index", now=self.now, api=self.api, **kwargs)

    def test_preserves_overnight_premarket_and_partial_responses_in_latest_24h(self):
        points = [(self.now - timedelta(hours=25), 1),
                  (self.now - timedelta(hours=24), 2),
                  (datetime(2026, 11, 26, 0), 3),
                  (datetime(2026, 11, 26, 12, tzinfo=UTC), 4),
                  (self.now, 5), (self.now, 6)]
        self.session.nextEvent.side_effect = [response(points[:3], 1), response(points[3:])]
        result = self.fetch()
        self.assertEqual(result["prices"], [2, 3, 4, 6])
        self.assertIn("2026-11-26T00:00:00Z", result["dates"])
        self.assertTrue(result["all_sessions"])
        self.query.set.assert_any_call("eventType", "TRADE")
        self.query.set.assert_any_call("startDateTime", (self.now - timedelta(days=7)).replace(tzinfo=None))
        self.query.set.assert_any_call("gapFillInitialBar", False)
        self.session.stop.assert_called_once()

    def test_long_weekend_uses_latest_available_session(self):
        last = self.now - timedelta(days=4)
        self.session.nextEvent.return_value = response([(last - timedelta(hours=8), 10), (last, 11)])
        self.assertEqual(self.fetch(interval=5)["prices"], [10, 11])
        self.query.set.assert_any_call("interval", 5)

    def test_rejects_bad_requests_before_opening_session(self):
        for interval in [0, 60, True, "1"]:
            with self.assertRaises(ValueError):
                self.fetch(interval=interval)
        for symbol in [None, "", "a\nb"]:
            with self.assertRaises(ValueError):
                fetch_intraday_bars(self.client, symbol)
        self.client.open_refdata_session.assert_not_called()

    def test_does_not_fill_missing_or_invalid_trades(self):
        self.session.nextEvent.return_value = response([(self.now, float("nan")), (self.now - timedelta(minutes=1), -1)])
        self.assertEqual(self.fetch()["prices"], [-1])

    def test_provider_errors_and_timeouts_stop_session(self):
        self.session.nextEvent.return_value = response([], error="Not entitled")
        with self.assertRaisesRegex(RuntimeError, "Not entitled"):
            self.fetch()
        self.session.stop.assert_called_once()
        self.session.stop.reset_mock()
        with self.assertRaises(TimeoutError):
            self.fetch(timeout_seconds=0)
        self.session.stop.assert_called_once()

    def test_route_requires_service_credential(self):
        app = Flask(__name__)
        register_intraday_routes(app, client=self.client, service_key="test-key")
        api = app.test_client()
        self.assertEqual(api.post("/bloomberg/intraday", json={"symbol": "ESA Index"}).status_code, 401)
        with patch("bloomberg.intraday.fetch_intraday_bars", return_value={"dates": [], "prices": []}) as fetch:
            result = api.post("/bloomberg/intraday", json={"symbol": "ESA Index", "interval": 1},
                              headers={"Authorization": "Bearer test-key"})
            self.assertEqual(result.status_code, 200)
            fetch.assert_called_once_with(self.client, "ESA Index", 1)


if __name__ == "__main__":
    unittest.main()
