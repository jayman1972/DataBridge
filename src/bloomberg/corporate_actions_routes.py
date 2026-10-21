"""Authenticated corporate-action route with a persistent, short-lived cache."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from flask import jsonify, request

from .corporate_actions_jobs import JobManager, QueueFullError
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
    app: Any, *, client: Any, service_key: str, job_manager: JobManager | None = None
) -> None:
    manager_lock = threading.Lock()

    def manager() -> JobManager:
        nonlocal job_manager
        with manager_lock:
            if job_manager is None:
                root = (
                    Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
                    / "EHP"
                    / "DataBridge"
                    / "corporate-actions-jobs"
                )
                job_manager = JobManager(root, client)
            return job_manager

    def authorized() -> bool:
        token = (
            (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        )
        return bool(
            token and service_key and secrets.compare_digest(token, service_key)
        )

    @app.route("/bloomberg/corporate-actions-jobs", methods=["POST"])
    def corporate_actions_jobs():
        if not authorized():
            return jsonify({"error": "Service authorization is required"}), 401
        # This header is set by the authenticated edge function, never trusted
        # without the server credential checked above.
        owner = request.headers.get("X-Requester-Id", "")
        if not re.fullmatch(
            r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}|service_role", owner
        ):
            return jsonify({"error": "An authenticated requester is required"}), 400
        if request.content_length and request.content_length > 16_384:
            return jsonify({"error": "Request exceeds 16 KB"}), 413
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "A JSON object is required"}), 400
        operation = payload.get("operation")
        try:
            if operation == "start":
                if client is None:
                    return jsonify({"error": "Bloomberg is unavailable"}), 503
                result = manager().create(
                    owner, payload.get("request_id"), payload.get("request")
                )
                return jsonify(result), 202
            job_id = payload.get("job_id", "")
            if not isinstance(job_id, str) or not re.fullmatch(r"[a-f0-9]{32}", job_id):
                raise ValueError("A valid job_id is required")
            if operation == "status":
                return jsonify(manager().get(owner, job_id))
            if operation == "cancel":
                return jsonify(manager().cancel(owner, job_id))
            raise ValueError("Invalid job operation")
        except (ValueError, TypeError) as exc:
            return jsonify({"error": str(exc)}), 400
        except KeyError:
            return jsonify({"error": "Corporate-action job not found"}), 404
        except QueueFullError as exc:
            return jsonify({"error": str(exc)}), 429
        except Exception:
            LOGGER.exception("Corporate-action job request failed")
            return jsonify({"error": "Could not access the Bloomberg job queue"}), 502

    @app.route("/bloomberg/corporate-actions-lookup", methods=["POST"])
    def corporate_actions_lookup():
        if not authorized():
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
