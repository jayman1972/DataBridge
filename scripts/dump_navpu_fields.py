"""List all NAV/NAVPU-related GetNAVSheet fields for USD share classes."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

env_path = ROOT.parent / "market-dashboard" / "bloomberg-service.env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))

from sggg.diamond_client import get_diamond_client
from sggg.nav_sheet_parse import _is_usd_share_class, _parse_class_navpu, _parse_money_value

_NAV_RE = re.compile(r"nav|price|unit|pu|currency|exchange|fx|asset", re.I)


def dump_class(entry: dict, fund_base: str) -> None:
    code = (entry.get("ClassCode") or "").strip()
    cid = (entry.get("FundID") or "").strip()
    if not _is_usd_share_class(code, cid):
        return
    navpu, ccy = _parse_class_navpu(entry, fund_base)
    print(f"\n--- Class {code} (parsed navpu={navpu}, ccy={ccy}) ---")
    print("  top-level keys:", sorted(k for k in entry if entry.get(k) not in (None, "", [])))
    for k in (
        "NAVPU",
        "LocalNAVPU",
        "FXRate",
        "Price",
        "Currency",
        "ClassCurrency",
        "NetAssetValue",
        "Units",
        "OpeningUnits",
        "OpeningEquity",
    ):
        if k in entry:
            print(f"  entry[{k!r}] = {entry.get(k)!r}")
    for sec in entry.get("SectionList") or []:
        sname = (sec.get("SectionName") or "").strip()
        items = sec.get("SectionItem")
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = (item.get("Name") or "").strip()
            val = item.get("Value")
            if not name:
                continue
            if _NAV_RE.search(name) or _NAV_RE.search(sname):
                print(f"  [{sname}] {name} = {val!r}")


def main() -> None:
    client = get_diamond_client()
    if not client:
        print("NO_CLIENT")
        return
    funds = [
        ("415a3530-3034-4536-4432-303030364337", "Alpha", "2026-05-20"),
        ("41010000-7F7A-0A65-D559-45484608DB40", "Tact", "2026-05-20"),
    ]
    for fid, label, d in funds:
        print(f"\n========== {label} {d} ==========")
        raw = client.get_nav_sheet(fid, d)
        body = raw.get("GetNAVSheetResponse", raw)
        print("FundCurrency:", body.get("FundCurrency"))
        for sec in body.get("SectionList") or []:
            sname = (sec.get("SectionName") or "").strip()
            for item in sec.get("SectionItem") or []:
                if not isinstance(item, dict):
                    continue
                name = (item.get("Name") or "").strip()
                if _NAV_RE.search(name) or "FX" in name.upper() or "EXCHANGE" in name.upper():
                    print(f"  FUND [{sname}] {name} = {item.get('Value')!r}")
        for entry in body.get("ClassSeriesFundList") or []:
            if isinstance(entry, dict):
                dump_class(entry, "CAD")


if __name__ == "__main__":
    main()
