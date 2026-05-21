import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
env_path = Path(__file__).resolve().parents[2] / "market-dashboard" / "bloomberg-service.env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))

from sggg.diamond_client import get_diamond_client
from sggg.nav_sheet_parse import list_capital_flow_candidates, parse_nav_sheet_summary

client = get_diamond_client()
raw = client.get_nav_sheet("415a3530-3034-4536-4432-303030364337", "2026-05-20")
body = raw.get("GetNAVSheetResponse", raw)
summary = parse_nav_sheet_summary(raw)
print("capital_flow", summary.get("capital_flow"), summary.get("capital_flow_label"))
cands = list_capital_flow_candidates(body)
print("candidates", len(cands), "sum", sum(c["amount"] for c in cands))
for c in cands:
    print(c)
