"""Bounded, authenticated Bloomberg intraday trade bars, including all sessions."""

from __future__ import annotations

import logging
import math
import secrets
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from flask import jsonify, request

LOGGER = logging.getLogger(__name__)


def fetch_intraday_bars(
    client: Any, symbol: str, interval: int = 1, *, now: datetime | None = None,
    api: Any = None, timeout_seconds: float = 25,
) -> dict[str, Any]:
    if not isinstance(symbol, str) or not symbol.strip() or len(symbol) > 160:
        raise ValueError("One Bloomberg security is required")
    if any(ord(char) < 32 for char in symbol):
        raise ValueError("Invalid Bloomberg security")
    if type(interval) is not int or interval not in (1, 5):
        raise ValueError("Interval must be 1 or 5 minutes")
    if api is None:
        import blpapi as api

    end = (now or datetime.now(UTC)).astimezone(UTC)
    # Seven days cover long weekends; display only the latest available 24h.
    start = end - timedelta(days=7)
    session = client.open_refdata_session()
    if session is None:
        raise RuntimeError("Bloomberg intraday session is unavailable")
    points: dict[datetime, float] = {}
    try:
        query = session.getService("//blp/refdata").createRequest("IntradayBarRequest")
        query.set("security", symbol.strip())
        query.set("eventType", "TRADE")
        query.set("interval", interval)
        # Bloomberg request times are GMT; no regular-session-only filter.
        query.set("startDateTime", start.replace(tzinfo=None))
        query.set("endDateTime", end.replace(tzinfo=None))
        query.set("gapFillInitialBar", False)
        session.sendRequest(query)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            event = session.nextEvent(500)
            for message in event:
                if message.hasElement("responseError"):
                    raise RuntimeError(str(message.getElement("responseError")))
                if str(message.messageType()) == "RequestFailure":
                    raise RuntimeError(str(message))
                if not message.hasElement("barData"):
                    continue
                bars = message.getElement("barData").getElement("barTickData")
                for index in range(bars.numValues()):
                    bar = bars.getValueAsElement(index)
                    stamp = bar.getElementAsDatetime("time")
                    if not isinstance(stamp, datetime):
                        continue
                    stamp = stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
                    price = bar.getElementAsFloat("close")
                    if start <= stamp <= end and math.isfinite(price):
                        points[stamp] = price
            if event.eventType() == api.Event.RESPONSE:
                break
        else:
            raise TimeoutError("Bloomberg intraday request timed out")
    finally:
        session.stop()
    cutoff = max(points) - timedelta(hours=24) if points else start
    rows = sorted((stamp, price) for stamp, price in points.items() if stamp >= cutoff)
    return {
        "symbol": symbol.strip(), "interval_minutes": interval, "all_sessions": True,
        "dates": [stamp.isoformat().replace("+00:00", "Z") for stamp, _ in rows],
        "prices": [price for _, price in rows],
    }


def register_intraday_routes(app: Any, *, client: Any, service_key: str) -> None:
    @app.route("/bloomberg/intraday", methods=["POST"])
    def bloomberg_intraday():
        token = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        if not token or not service_key or not secrets.compare_digest(token, service_key):
            return jsonify({"error": "Service authorization is required"}), 401
        if request.content_length and request.content_length > 4096:
            return jsonify({"error": "Request exceeds 4 KB"}), 413
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "A JSON object is required"}), 400
        if client is None:
            return jsonify({"error": "Bloomberg is unavailable"}), 503
        try:
            return jsonify(fetch_intraday_bars(client, payload.get("symbol"), payload.get("interval", 1)))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TimeoutError as exc:
            return jsonify({"error": str(exc)}), 504
        except Exception as exc:
            LOGGER.exception("Bloomberg intraday request failed")
            return jsonify({"error": str(exc)}), 502
