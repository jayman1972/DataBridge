"""Authenticated production routes layered onto the legacy DataBridge app."""

from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, request

from .alphadesk_security_coverage import fetch_alphadesk_security_coverage
from .trade_file_export import DEFAULT_TRADE_EXPORT_DIR, save_alphadesk_trade_file

_LOGGER = logging.getLogger("data_bridge.sggg.production")


def _bearer_token() -> str:
    authorization = request.headers.get("Authorization") or ""
    return authorization[7:].strip() if authorization.startswith("Bearer ") else ""


def _authenticated_admin_user_id(
    supabase_client: Any,
    supabase_url: str,
    service_key: str,
) -> tuple[str | None, str | None, int]:
    token = _bearer_token()
    if not token:
        return None, "A signed-in administrator session is required.", 401
    if not supabase_client or not supabase_url or not service_key:
        return None, "Supabase is not configured.", 503
    try:
        auth_response = requests.get(
            f"{supabase_url.rstrip('/')}/auth/v1/user",
            headers={"apikey": service_key, "Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if auth_response.status_code != 200:
            return None, "The administrator session is invalid or expired.", 401
        user_id = str((auth_response.json() or {}).get("id") or "").strip()
        if not user_id:
            return None, "The administrator session could not be identified.", 401
        role_result = supabase_client.rpc(
            "has_role", {"_user_id": user_id, "_role": "admin"}
        ).execute()
        if role_result.data is not True:
            return None, "Administrator access is required.", 403
        return user_id, None, 200
    except requests.RequestException:
        _LOGGER.exception("Unable to validate AlphaDesk trade-export authorization")
        return None, "Administrator authorization could not be verified.", 503


def register_sggg_production_routes(
    app: Flask,
    *,
    supabase_client: Any,
    supabase_url: str,
    service_key: str,
) -> None:
    """Register narrowly scoped AlphaDesk production integration routes."""

    @app.route("/sggg/alphadesk-trade-file", methods=["POST"])
    def alphadesk_trade_file():
        user_id, auth_error, auth_status = _authenticated_admin_user_id(
            supabase_client,
            supabase_url,
            service_key,
        )
        if auth_error:
            return jsonify({"error": auth_error}), auth_status
        if request.content_length and request.content_length > 6 * 1024 * 1024:
            return jsonify({"error": "Trade export request exceeds 6 MB."}), 413
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "A JSON request body is required."}), 400
        filename = data.get("filename")
        csv_content = data.get("csv")
        payload_sha256 = data.get("payload_sha256")
        if not all(
            isinstance(value, str)
            for value in (filename, csv_content, payload_sha256)
        ):
            return (
                jsonify(
                    {"error": "filename, csv, and payload_sha256 are required strings."}
                ),
                400,
            )
        target_dir = Path(
            os.environ.get("SGGG_TRADE_EXPORT_DIR", str(DEFAULT_TRADE_EXPORT_DIR))
        )
        try:
            result = save_alphadesk_trade_file(
                filename=filename,
                csv_content=csv_content,
                expected_sha256=payload_sha256,
                target_dir=target_dir,
            )
            _LOGGER.info(
                "AlphaDesk trade file saved user=%s filename=%s bytes=%s overwritten=%s",
                user_id,
                result["filename"],
                result["bytes"],
                result["overwritten"],
            )
            return jsonify({"success": True, **result}), 200
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except OSError as exc:
            _LOGGER.exception("Unable to write AlphaDesk production trade file")
            return (
                jsonify(
                    {"error": f"The AlphaDesk trade file could not be saved: {exc}"}
                ),
                500,
            )

    @app.route("/sggg/alphadesk-security-coverage", methods=["POST"])
    def alphadesk_security_coverage():
        token = _bearer_token()
        if not token or not service_key or not secrets.compare_digest(token, service_key):
            return jsonify({"error": "Service authorization is required."}), 401
        data = request.get_json(silent=True)
        securities = data.get("securities") if isinstance(data, dict) else None
        if not isinstance(securities, list):
            return jsonify({"error": "securities must be an array."}), 400
        try:
            import pyodbc

            connection = pyodbc.connect("DSN=PSC_VIEWER", timeout=15)
            try:
                result = fetch_alphadesk_security_coverage(connection, securities)
            finally:
                connection.close()
            return jsonify({"success": True, **result}), 200
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except (ImportError, OSError, RuntimeError):
            _LOGGER.exception("AlphaDesk security coverage check failed")
            return jsonify({"error": "AlphaDesk security coverage is unavailable."}), 503
        except Exception:
            _LOGGER.exception("AlphaDesk ODBC security coverage query failed")
            return jsonify({"error": "AlphaDesk security coverage query failed."}), 502
