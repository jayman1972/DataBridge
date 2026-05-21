"""Debug capital flow parsing per fund/date."""
from __future__ import annotations

import os
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
from sggg.nav_sheet_parse import (
    capital_flow_net_from_summary,
    list_capital_flow_candidates,
    parse_nav_sheet_summary,
    sggg_opening_aum_from_prior_summary,
)

FUNDS = [
    ("415a3530-3034-4536-4432-303030364337", "Alpha"),
    ("41010000-7F7A-0A65-D559-45484608DB40", "Tact"),
    ("01010000-801A-4995-8370-45484608DE57", "Expo"),
]


def show(fid: str, name: str, d: str) -> None:
    raw = get_diamond_client().get_nav_sheet(fid, d)
    s = parse_nav_sheet_summary(raw)
    flow, label = capital_flow_net_from_summary(s)
    cands = list_capital_flow_candidates(
        raw.get("GetNAVSheetResponse", raw)
    )
    print(f"\n{name} {d}: EOD={s.get('fund_aum_closing')} flow={flow} ({label})")
    for c in cands:
        if abs(float(c["amount"])) > 1000:
            print(f"  {c['scope']} {c.get('class_code')} {c['name']} = {c['amount']}")


def main() -> None:
    c = get_diamond_client()
    if not c:
        print("NO_CLIENT")
        return
    for fid, name in FUNDS:
        for d in ("2026-05-19", "2026-05-20"):
            try:
                show(fid, name, d)
            except Exception as exc:
                print(name, d, "ERR", exc)
        prior = c.get_nav_sheet(fid, "2026-05-19")
        close = c.get_nav_sheet(fid, "2026-05-20")
        ps = parse_nav_sheet_summary(prior)
        cs = parse_nav_sheet_summary(close)
        op, eod, pf, _ = sggg_opening_aum_from_prior_summary(ps, "2026-05-19")
        cf, _ = capital_flow_net_from_summary(cs)
        cl = cs.get("fund_aum_closing")
        print(
            f"\n{name} May20 SGGG model: open={op} (eod={eod}+pf={pf}) close={cl} "
            f"report_flow={cf} => {float(cl) - float(op) - float(cf or 0):,.0f}"
        )


if __name__ == "__main__":
    main()
