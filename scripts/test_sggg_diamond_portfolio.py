#!/usr/bin/env python3
"""
Test a single SGGG Diamond GetPortfolio call with an explicit payload.

Run from the DataBridge folder:

    python scripts/test_sggg_diamond_portfolio.py

This uses the same env loading pattern as test_sggg_diamond_connection.py, and
then calls GetPortfolio with the GUID and dates you specify below.
"""

import os
import sys
from pprint import pprint


_script_dir = os.path.dirname(os.path.abspath(__file__))
_data_bridge_root = os.path.dirname(_script_dir)
if _data_bridge_root not in sys.path:
    sys.path.insert(0, _data_bridge_root)


def _load_env():
    """
    Load SGGG Diamond env (username/password) from bloomberg-service.env
    in DataBridge or market-dashboard.
    """
    for _config_dir in [
        _data_bridge_root,
        os.path.normpath(os.path.join(_data_bridge_root, "..", "market-dashboard")),
    ]:
        _cfg = os.path.join(_config_dir, "bloomberg-service.env")
        if os.path.exists(_cfg):
            try:
                with open(_cfg, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            k, v = line.split("=", 1)
                            k, v = k.strip(), v.strip()
                            if k and v:
                                os.environ[k] = v
            except Exception as e:  # pragma: no cover - diagnostics only
                print(f"Warning: could not read {_cfg}: {e}")
            break


def main():
    # Hard-coded payload matching SGGG support example.
    # Adjust these values as needed.
    fund_id = "415a3530-3034-4536-4432-303030364337"
    valuation_date = "2026-03-10"
    reference_date = "2026-01-01"
    exclude_not_priced_positions = False
    exclude_flat_positions = False

    _load_env()

    try:
        from src.sggg.diamond_client import get_diamond_client
    except ImportError:
        try:
            from diamond_client import get_diamond_client
        except ImportError:
            print("FAIL: Could not import diamond_client. Run from DataBridge folder.")
            sys.exit(1)

    client = get_diamond_client()
    if not client:
        print(
            "FAIL: Diamond API not configured. "
            "Set SGGG_DIAMOND_USERNAME and SGGG_DIAMOND_PASSWORD in bloomberg-service.env"
        )
        sys.exit(1)

    try:
        result = client.get_portfolio(
            fund_id=fund_id,
            valuation_date=valuation_date,
            reference_date=reference_date,
            exclude_flat_positions=exclude_flat_positions,
            exclude_not_priced_positions=exclude_not_priced_positions,
        )
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

    print("OK: GetPortfolio response received.")
    if isinstance(result, dict):
        keys = list(result.keys())
        print(f"Top-level keys: {keys}")
        # Print a small sample to avoid dumping huge payloads.
        if "Positions" in result and isinstance(result["Positions"], list):
            print(f"Positions count: {len(result['Positions'])}")
            if result["Positions"]:
                print("First position:")
                pprint(result["Positions"][0])
        else:
            pprint(result)
    else:
        pprint(result)


if __name__ == "__main__":
    main()

