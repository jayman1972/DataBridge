import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from sggg.nav_sheet_parse import sggg_opening_aum_from_prior_summary

prior = {
    "fund_aum_closing": 33_314_916.0,
    "capital_flow": 1_530_000.0,
}
opening, eod, flow, _ = sggg_opening_aum_from_prior_summary(prior, "2026-05-19")
assert abs(opening - 33_314_916.0) < 1.0
assert abs(eod - 33_314_916.0) < 1.0
assert opening == eod
print("ok", opening)
