"""Authenticated corporate-action route with a persistent, short-lived cache."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from flask import jsonify, request

from .corporate_actions_lookup import lookup, validate_request

LOGGER = logging.getLogger("data_bridge.bloomberg.corporate_actions.routes")
LOOKUP_LOCK = threading.Lock()


def cached_lookup(
    client: Any, options: dict[str, Any], cache_path: Path, *, refresh: bool = False
) -> dict[str, Any]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_key = hashlib.sha256(json.dumps(options, sort_keys=True).encode()).hexdigest()
    with closing(sqlite3.connect(cache_path, timeout=5)) as connection, connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS lookups (cache_key TEXT, fetched REAL, payload TEXT, PRIMARY KEY(cache_key, fetched))"
        )
        if not refresh:
            row = connection.execute(
                "SELECT payload FROM lookups WHERE cache_key = ? AND fetched >= ? ORDER BY fetched DESC LIMIT 1",
                (cache_key, time.time() - 300),
            ).fetchone()
            if row:
                LOGGER.info(
                    "Corporate action cache hit source=%d range=%s..%s",
                    len(options["securities"]),
                    options["start_date"],
                    options["end_date"],
                )
                return {**json.loads(row[0]), "cached": True}
    result = lookup(client, options)
    if not result["errors"]:
        with closing(sqlite3.connect(cache_path, timeout=5)) as connection, connection:
            connection.execute(
                "INSERT INTO lookups VALUES (?, ?, ?)",
                (cache_key, time.time(), json.dumps(result)),
            )
    return {**result, "cached": False}


def register_corporate_action_routes(
    app: Any, *, client: Any, service_key: str
) -> None:
    @app.route("/bloomberg/corporate-actions-lookup", methods=["POST"])
    def corporate_actions_lookup():
        token = (
            (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        )
        if (
            not token
            or not service_key
            or not secrets.compare_digest(token, service_key)
        ):
            return jsonify({"error": "Service authorization is required"}), 401
        if client is None:
            return jsonify({"error": "Bloomberg is unavailable"}), 503
        if request.content_length and request.content_length > 16_384:
            return jsonify({"error": "Request exceeds 16 KB"}), 413
        try:
            payload = request.get_json(silent=True)
            options = validate_request(payload)
        except (TypeError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400
        if not LOOKUP_LOCK.acquire(blocking=False):
            return jsonify(
                {
                    "error": "Another corporate-action lookup is running. Please retry shortly."
                }
            ), 429
        try:
            cache_path = (
                Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
                / "EHP"
                / "DataBridge"
                / "corporate-actions.sqlite3"
            )
            result = cached_lookup(
                client, options, cache_path, refresh=payload.get("refresh") is True
            )
            return jsonify(result), 200
        except Exception:
            LOGGER.exception("Corporate-action lookup failed")
            return jsonify(
                {
                    "error": "Bloomberg lookup failed. Check Bloomberg login and DataBridge logs."
                }
            ), 502
        finally:
            LOOKUP_LOCK.release()
