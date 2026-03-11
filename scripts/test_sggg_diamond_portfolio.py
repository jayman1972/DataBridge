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
import argparse
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
    parser = argparse.ArgumentParser(
        description="Test SGGG Diamond GetPortfolio using an explicit payload."
    )
    parser.add_argument(
        "--username",
        help="Override SGGG_DIAMOND_USERNAME. Default matches support example.",
        default="API@EHPARTNERS.COM",
    )
    parser.add_argument(
        "--password",
        help="Override SGGG_DIAMOND_PASSWORD (avoid committing secrets to files).",
        default="",
    )
    parser.add_argument(
        "--fund-id",
        help="Override FundID (parent GUID).",
        default="415a3530-3034-4536-4432-303030364337",
    )
    parser.add_argument(
        "--valuation-date",
        help="Override ValuationDate (yyyy-mm-dd).",
        default="2026-03-10",
    )
    parser.add_argument(
        "--reference-date",
        help="Override ReferenceDate (yyyy-mm-dd).",
        default="2026-01-01",
    )
    parser.add_argument(
        "--exclude-not-priced-positions",
        action="store_true",
        help="Set ExcludeNotPricedPositions=true (default false to match support payload).",
    )
    parser.add_argument(
        "--exclude-flat-positions",
        action="store_true",
        help="Set ExcludeFlatPositions=true (default false to match support payload).",
    )
    args = parser.parse_args()

    # Hard-coded payload matching SGGG support example.
    # Adjust these values as needed.
    fund_id = args.fund_id
    valuation_date = args.valuation_date
    reference_date = args.reference_date
    exclude_not_priced_positions = bool(args.exclude_not_priced_positions)
    exclude_flat_positions = bool(args.exclude_flat_positions)

    _load_env()

    try:
        from src.sggg.diamond_client import get_diamond_client, BASE_URL
    except ImportError:
        try:
            from diamond_client import get_diamond_client  # type: ignore
            BASE_URL = "https://api.sgggfsi.com/api/v1"  # fallback
        except ImportError:
            print("FAIL: Could not import diamond_client. Run from DataBridge folder.")
            sys.exit(1)

    if args.username:
        os.environ["SGGG_DIAMOND_USERNAME"] = args.username
    if args.password:
        os.environ["SGGG_DIAMOND_PASSWORD"] = args.password

    client = get_diamond_client()
    if not client:
        print(
            "FAIL: Diamond API not configured. "
            "Set SGGG_DIAMOND_USERNAME and SGGG_DIAMOND_PASSWORD in bloomberg-service.env"
        )
        sys.exit(1)

    username = getattr(client, "username", None) or os.environ.get("SGGG_DIAMOND_USERNAME", "")
    print(f"Using username: {username}")
    print(f"Base URL: {getattr(client, 'base_url', BASE_URL)}")
    print("Payload:")
    pprint(
        {
            "ExcludeNotPricedPositions": exclude_not_priced_positions,
            "ValuationDate": valuation_date,
            "FundID": fund_id,
            "ReferenceDate": reference_date,
            "ExcludeFlatPositions": exclude_flat_positions,
        }
    )

    try:
        result = client.get_portfolio(
            fund_id=fund_id,
            valuation_date=valuation_date,
            reference_date=reference_date,
            exclude_flat_positions=exclude_flat_positions,
            exclude_not_priced_positions=exclude_not_priced_positions,
        )
    except Exception as e:
        # requests raises an HTTPError with message like:
        # "401 Client Error: Unauthorized for url: ..."
        # That text is created client-side by the requests library.
        resp = getattr(e, "response", None)
        print(f"FAIL: {e}")
        if resp is not None:
            try:
                print(f"HTTP status: {resp.status_code}")
                print("Response body:")
                print(resp.text)
            except Exception:
                pass
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

