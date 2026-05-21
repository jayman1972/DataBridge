"""Dump money fields from Diamond GetNAVSheet for debugging subs/AUM."""
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
from sggg.nav_sheet_parse import _parse_money_value, _section_items_by_name, parse_nav_sheet_summary

_CAPITAL_RE = re.compile(
    r"subscr|redemp|subs|capital|flow|contribut|withdraw|transfer|unit",
    re.IGNORECASE,
)


def walk_money(body, prefix: str = "") -> list:
    rows = []
    if not isinstance(body, dict):
        return rows
    for sec in body.get("SectionList") or []:
        sname = sec.get("SectionName") or ""
        for item in sec.get("SectionItem") or []:
            name = (item.get("Name") or "").strip()
            val = item.get("Value")
            m = _parse_money_value(val)
            if m is not None and abs(m) > 1000:
                rows.append((f"{prefix}{sname}", name, m, val))
    for entry in body.get("ClassSeriesFundList") or []:
        code = entry.get("ClassCode", "")
        for sec in entry.get("SectionList") or []:
            sname = sec.get("SectionName") or ""
            for item in sec.get("SectionItem") or []:
                name = (item.get("Name") or "").strip()
                val = item.get("Value")
                m = _parse_money_value(val)
                if m is not None and abs(m) > 1000:
                    rows.append((f"class {code}/{sname}", name, m, val))
    return rows


def main() -> None:
    client = get_diamond_client()
    if not client:
        print("NO_CLIENT")
        return
    fid = "415a3530-3034-4536-4432-303030364337"
    for d in ["2026-05-18", "2026-05-19", "2026-05-20"]:
        print("===", d, "===")
        try:
            raw = client.get_nav_sheet(fid, d)
            body = raw.get("GetNAVSheetResponse", raw)
            summary = parse_nav_sheet_summary(raw)
            print(
                "sheet_date",
                body.get("ValuationDate"),
                "root NAV",
                body.get("NetAssetValue"),
                "parsed native",
                summary.get("net_asset_value_native"),
                "capital_flow",
                summary.get("capital_flow"),
                summary.get("capital_flow_label"),
            )
            for k, v in sorted(_section_items_by_name(body).items()):
                m = _parse_money_value(v)
                if m is not None and abs(m) > 1000:
                    print("  FUND", k, m)
            cap = [
                (s, n, m)
                for s, n, m, _ in walk_money(body)
                if _CAPITAL_RE.search(n) or _CAPITAL_RE.search(s)
            ]
            print("  CAPITAL-LIKE", len(cap))
            for s, n, m in sorted(cap, key=lambda x: -abs(x[2]))[:30]:
                print("   ", s, n, "=", m)
            big = [(s, n, m) for s, n, m, _ in walk_money(body) if abs(m) > 500_000]
            print("  TOP MONEY (>500k):")
            for s, n, m in sorted(big, key=lambda x: -abs(x[2]))[:20]:
                print("   ", s, n, "=", round(m, 2))
        except Exception as exc:
            print("ERR", exc)


if __name__ == "__main__":
    main()
