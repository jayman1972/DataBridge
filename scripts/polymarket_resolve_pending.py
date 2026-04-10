#!/usr/bin/env python3
"""
Poll Polymarket Gamma API for resolved markets and update at_polymarket_alerts.outcome_status.

Run on a schedule (cron / Task Scheduler), e.g. every hour:
  cd path/to/DataBridge
  python scripts/polymarket_resolve_pending.py

Requires same env as data_bridge.py: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
(read from bloomberg-service.env next to DataBridge or market-dashboard).

Outcome rules (simplified):
  - When market is closed, winning outcome = index with highest outcomePrices (ties: first max).
  - BUY on outcome i: won if i == winning index; else lost.
  - SELL on outcome i: won if i != winning index (short the outcome); else lost.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

# Load bloomberg-service.env (same search order as data_bridge)
_script_dir = Path(__file__).resolve().parent.parent
_projects = _script_dir.parent
for cfg in (
    _script_dir / "bloomberg-service.env",
    _projects / "market-dashboard" / "bloomberg-service.env",
):
    if cfg.exists():
        with open(cfg, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k and k not in os.environ:
                        os.environ[k] = v
        break

try:
    from supabase import create_client
except ImportError:
    print("Install: pip install supabase", file=sys.stderr)
    sys.exit(1)

GAMMA = "https://gamma-api.polymarket.com"


def _load_supabase():
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        print("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        sys.exit(1)
    return create_client(url, key)


def _winning_index(market: dict) -> int | None:
    if not market.get("closed"):
        return None
    try:
        prices = json.loads(market["outcomePrices"])
    except (json.JSONDecodeError, KeyError, TypeError):
        return None
    if not prices:
        return None
    best = 0
    best_p = -1.0
    for i, p in enumerate(prices):
        try:
            fv = float(p)
        except (TypeError, ValueError):
            fv = 0.0
        if fv > best_p:
            best_p = fv
            best = i
    return best


def _resolve_alert_outcome(
    *,
    trade_side: str,
    outcome_index: int,
    winning_idx: int | None,
) -> str:
    if winning_idx is None:
        return "unknown"
    side = (trade_side or "BUY").upper()
    if side == "BUY":
        return "won" if outcome_index == winning_idx else "lost"
    if side == "SELL":
        return "won" if outcome_index != winning_idx else "lost"
    return "unknown"


def main() -> None:
    sb = _load_supabase()
    res = (
        sb.table("at_polymarket_alerts")
        .select("id,market_id,trade_side,outcome_status,payload")
        .eq("outcome_status", "pending")
        .limit(200)
        .execute()
    )
    rows = res.data or []
    if not rows:
        print("No pending alerts.")
        return

    # Cache Gamma responses per condition id
    market_cache: dict[str, dict] = {}
    updated = 0

    for row in rows:
        mid = row.get("market_id") or ""
        if not mid:
            continue
        if mid not in market_cache:
            r = requests.get(f"{GAMMA}/markets", params={"condition_id": mid, "limit": 1}, timeout=30)
            if r.status_code != 200:
                market_cache[mid] = {}
                continue
            data = r.json()
            market_cache[mid] = data[0] if isinstance(data, list) and data else {}

        m = market_cache[mid]
        if not m:
            continue

        if not m.get("closed"):
            continue

        win_idx = _winning_index(m)

        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        oi = payload.get("outcome_index")
        if oi is None:
            oi = 0
        try:
            oi = int(oi)
        except (TypeError, ValueError):
            oi = 0
        ts = str(payload.get("trade_side") or row.get("trade_side") or "BUY")

        if win_idx is None:
            status = "unknown"
        else:
            status = _resolve_alert_outcome(
                trade_side=ts, outcome_index=oi, winning_idx=win_idx
            )

        sb.table("at_polymarket_alerts").update({"outcome_status": status}).eq("id", row["id"]).execute()

        res_out = None
        try:
            outs = json.loads(m["outcomes"])
            if win_idx is not None and win_idx < len(outs):
                res_out = str(outs[win_idx])
        except (json.JSONDecodeError, TypeError, KeyError, IndexError):
            pass

        sb.table("at_polymarket_markets").upsert(
            {
                "condition_id": mid,
                "title": m.get("question"),
                "resolved_at": m.get("closedTime"),
                "resolution_outcome": res_out,
                "raw": None,
            },
            on_conflict="condition_id",
        ).execute()
        updated += 1

    print(f"Updated {updated} alert(s).")


if __name__ == "__main__":
    main()
