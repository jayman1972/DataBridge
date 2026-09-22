"""Production DataBridge entry point with bounded ETF constituent routes."""

from __future__ import annotations

from datetime import date, datetime, timezone

from flask import jsonify, request

import data_bridge
from bloomberg.etf_constituents import (
    fetch_etf_constituent_snapshots,
    normalize_etf_tickers,
)
from sggg.production_routes import register_sggg_production_routes

data_bridge.DATA_BRIDGE_BUILD = "2026-09-22-alphadesk-identity-resolution"
register_sggg_production_routes(
    data_bridge.app,
    supabase_client=data_bridge.supabase,
    supabase_url=data_bridge.SUPABASE_URL,
    service_key=data_bridge.SUPABASE_KEY,
)


@data_bridge.app.route("/bloomberg/etf-constituents", methods=["POST"])
def bloomberg_etf_constituents():
    """Return benchmark diagnostics without mislabeling them as ETF holdings."""
    if data_bridge.bloomberg_client is None:
        return jsonify({"error": "Bloomberg client not available"}), 503
    if not hasattr(data_bridge.bloomberg_client, "get_bds_rows"):
        return jsonify({"error": "Bloomberg client does not support structured BDS"}), 501
    payload = request.get_json() or {}
    try:
        etfs = normalize_etf_tickers(payload.get("etfs"))
        as_of_raw = str(payload.get("asOfDate") or "").strip()
        as_of_date = (
            date.fromisoformat(as_of_raw)
            if as_of_raw
            else datetime.now(timezone.utc).date()
        )
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400
    if payload.get("allowIndexProxy") is not True:
        return (
            jsonify(
                {
                    "sourceProvider": "bloomberg_benchmark_index_proxy",
                    "snapshots": [],
                    "errors": [
                        (
                            "Exact Bloomberg ETF holdings require the BQL holdings() "
                            "entitlement; benchmark-index weights are intentionally "
                            "not published as ETF holdings"
                        )
                    ],
                }
            ),
            409,
        )
    result = fetch_etf_constituent_snapshots(
        data_bridge.bloomberg_client,
        etfs,
        as_of_date,
    )
    return jsonify(result), 200 if result["snapshots"] else 502


if __name__ == "__main__":
    data_bridge.app.run(
        host="127.0.0.1",
        port=data_bridge.SERVICE_PORT,
        debug=False,
        use_reloader=False,
    )
