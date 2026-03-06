#!/usr/bin/env python3
"""Test SGGG Diamond API (HTTP) connection. Run from DataBridge folder: python scripts/test_sggg_diamond_connection.py"""
import os
import sys

# DataBridge root = parent of scripts/
_script_dir = os.path.dirname(os.path.abspath(__file__))
_data_bridge_root = os.path.dirname(_script_dir)
if _data_bridge_root not in sys.path:
    sys.path.insert(0, _data_bridge_root)

# Load SGGG Diamond env from bloomberg-service.env (same as data_bridge.py)
for _config_dir in [_data_bridge_root, os.path.normpath(os.path.join(_data_bridge_root, "..", "market-dashboard"))]:
    _cfg = os.path.join(_config_dir, "bloomberg-service.env")
    if os.path.exists(_cfg):
        try:
            with open(_cfg, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip()
                        if k in ("SGGG_DIAMOND_USERNAME", "SGGG_DIAMOND_PASSWORD", "SGGG_DIAMOND_FUND_ID", "SGGG_DIAMOND_FUND_IDS") and v:
                            os.environ[k] = v
        except Exception as e:
            print(f"Warning: could not read {_cfg}: {e}")
        break

try:
    from src.sggg.diamond_client import get_diamond_client
except ImportError:
    try:
        from diamond_client import get_diamond_client
    except ImportError:
        print("FAIL: Could not import diamond_client. Run from DataBridge folder.")
        sys.exit(1)

def main():
    client = get_diamond_client()
    if not client:
        print("FAIL: Diamond API not configured. Set SGGG_DIAMOND_USERNAME and SGGG_DIAMOND_PASSWORD in bloomberg-service.env")
        sys.exit(1)
    try:
        client._ensure_auth()
        print("OK: SGGG Diamond API (HTTP) connection successful (https://api.sgggfsi.com).")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
