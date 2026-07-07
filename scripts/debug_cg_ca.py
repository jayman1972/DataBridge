"""One-off: dump Diamond GetPortfolio rows for CG.CA / Centerra."""
import json
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "src"))

env_path = Path(r"c:\Users\jmann\projects\market-dashboard\bloomberg-service.env")
for line in env_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from sggg.close_price_reconcile import flatten_diamond_portfolio_records, infer_reference_date_from_psc, normalize_valuation_date
from sggg.diamond_client import get_diamond_client

FIELDS = [
    "CompanySymbol",
    "SecurityName",
    "PricingTicker",
    "ISIN",
    "QuoteDate",
    "PortfolioPrice",
    "Quantity",
    "LongShort",
    "BaseTotalUnrealizedGainLoss",
    "BaseUnrealizedGainLossSinceReferenceDate",
    "BaseUnrealizedGainLossSinceReferenceDateDueToPriceChange",
    "BaseUnrealizedGainLossSinceReferenceDateDueToFX",
    "BaseRealizedGainLossSinceReferenceDate",
]


def is_cg_row(row: dict) -> bool:
    isin = str(row.get("ISIN") or "").upper()
    if isin == "CA1520061021":
        return True
    for key in ("CompanySymbol", "PricingTicker", "SecurityName"):
        val = str(row.get(key) or "").upper()
        if val in ("CG", "CG.CA") or "CENTERRA" in val:
            return True
    return False


def main() -> None:
    client = get_diamond_client()
    if not client:
        raise SystemExit("Diamond not configured")
    fund = "415a3530-3034-4536-4432-303030364337"
    vd = normalize_valuation_date("2026-07-06")
    ref, _, _ = infer_reference_date_from_psc(fund, vd)
    raw_t = client.get_portfolio(fund_id=fund, valuation_date=vd, reference_date=ref)
    raw_prior = client.get_portfolio(fund_id=fund, valuation_date=ref)
    out = {
        "valuation_date_T": vd,
        "valuation_date_prior": ref,
        "positions_T": [
            {f: row.get(f) for f in FIELDS}
            for row in flatten_diamond_portfolio_records(raw_t)
            if is_cg_row(row)
        ],
        "positions_prior": [
            {f: row.get(f) for f in FIELDS}
            for row in flatten_diamond_portfolio_records(raw_prior)
            if is_cg_row(row)
        ],
    }
    trades_raw = client.get_portfolio_trades(
        fund_parent_id=fund,
        start_date=ref,
        end_date=vd,
        date_type="ValuationDate",
    )
    from sggg.close_price_reconcile import flatten_diamond_trade_records

    out["trades_T"] = [
        {f: row.get(f) for f in [
            "Ticker", "SecurityName", "ValuationDate", "TradeDate", "Quantity",
            "IsReversal", "IsBackDated", "IsCancel", "TradeType",
        ]}
        for row in flatten_diamond_trade_records(trades_raw)
        if is_cg_row(row)
    ]
    path = root / "logs" / "nav_checker_debug_CG_CA_local.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
