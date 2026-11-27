"""Authenticated production routes layered onto the legacy DataBridge app."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, request

from .alphadesk_security_coverage import fetch_alphadesk_security_coverage
from .scotia_borrow import (
    DeliveryReviewRequired,
    deliver_scotia_locates,
    load_scotia_rows,
    locate_csv,
)
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


def verify_recorded_trade_file(
    client: Any, user_id: str, filename: str, digest: str
) -> None:
    """Require the caller's recorded file, without rerunning trade-sizing policy."""
    for table, owner in (
        ("bt_sggg_trade_batches", "created_by"),
        ("bt_sggg_trade_exports", "exported_by"),
    ):
        records = (
            client.table(table)
            .select("id")
            .eq(owner, user_id)
            .eq("filename", filename)
            .eq("payload_sha256", digest)
            .limit(1)
            .execute()
            .data
        )
        if records:
            return
    raise ValueError("A matching export record owned by your account is required.")


def register_sggg_production_routes(
    app: Flask,
    *,
    supabase_client: Any,
    supabase_url: str,
    service_key: str,
) -> None:
    """Register narrowly scoped AlphaDesk production integration routes."""

    @app.route("/sggg/scotia-borrow-requests", methods=["POST"])
    def scotia_borrow_requests():
        user_id, auth_error, auth_status = _authenticated_admin_user_id(
            supabase_client, supabase_url, service_key
        )
        if auth_error:
            return jsonify({"error": auth_error}), auth_status
        if request.content_length and request.content_length > 32 * 1024:
            return jsonify({"error": "Borrow request exceeds 32 KB."}), 413
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "A JSON request body is required."}), 400
        try:
            rows = load_scotia_rows(
                supabase_client, data.get("draft_ids"), data.get("trade_date")
            )
            expected = data.get("payload_sha256")
            actual = hashlib.sha256(locate_csv(rows)).hexdigest()
            if not isinstance(expected, str) or not secrets.compare_digest(
                actual, expected
            ):
                raise ValueError(
                    "The selected Scotia draft rows changed. Refresh the draft list and review them before sending."
                )
            result = deliver_scotia_locates(rows, data["trade_date"], user_id)
            return jsonify(result), 200
        except (ValueError, TypeError) as exc:
            return jsonify({"error": str(exc)}), 422
        except DeliveryReviewRequired as exc:
            return jsonify({"error": str(exc)}), 409
        except Exception:
            # Never expose the credentials from the legacy connection script.
            _LOGGER.exception("Scotia borrow request could not be delivered")
            return jsonify(
                {
                    "error": "Scotia delivery is unavailable. Check the bridge connection, shared drive and Scotia SFTP settings, then retry. No delivery was confirmed."
                }
            ), 503

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
            isinstance(value, str) for value in (filename, csv_content, payload_sha256)
        ):
            return (
                jsonify(
                    {"error": "filename, csv, and payload_sha256 are required strings."}
                ),
                400,
            )
        try:
            verify_recorded_trade_file(
                supabase_client, user_id, filename, payload_sha256
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 422
        except Exception:
            _LOGGER.exception("Trade export ownership verification failed")
            return jsonify(
                {
                    "error": "The saved export record could not be verified. Retry saving."
                }
            ), 503
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
        if (
            not token
            or not service_key
            or not secrets.compare_digest(token, service_key)
        ):
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
            return jsonify(
                {"error": "AlphaDesk security coverage is unavailable."}
            ), 503
        except Exception:
            _LOGGER.exception("AlphaDesk ODBC security coverage query failed")
            return jsonify({"error": "AlphaDesk security coverage query failed."}), 502
