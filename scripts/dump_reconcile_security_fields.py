"""
Dump PSC + (optional) Diamond ticker-related fields for one security.

Usage (VPN + PSC_VIEWER):
  python scripts/dump_reconcile_security_fields.py --portfolio "EHP Strat Inc Alt Fund" --date 2026-05-21 --search TIDEWATER
  python scripts/dump_reconcile_security_fields.py --portfolio "Expon Bal Grow Fund" --date 20260521 --search "QQQ"

Prints every label-related column on psc_position_history / psc_security_data so you can pick the display field.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (  # noqa: E402
    portfolio_details_display_ticker,
    _looks_like_bond_description,
    _looks_like_option_description,
    parse_option_contract_key,
)

PSC_LABEL_SQL = """
SELECT
  ph.SECURITY,
  ph.COMPANY_SYMBOL,
  ph.DESCRIPTION,
  ph.BBG_TICKER,
  ph.SECURITY_TYPE,
  ph.SEC_CCY,
  ph.EXCHANGE,
  ph.COUNTRY,
  ph.ISIN,
  ph.CUSIP,
  sd.SEDOL,
  ph.SECURITY_SN,
  ph.UNDERLYING_SECURITY,
  ph.UNDERLYING_COMPANY_SYMBOL,
  ph.UNDERLYING_DESCRIPTION,
  ph.LONG_SHORT,
  ph.QUANTITY,
  ph.CLOSE_PRICE
FROM psc_position_history ph
LEFT JOIN psc_security_data sd ON ph.security_sn = sd.security_sn
WHERE ph.PORTFOLIO LIKE ?
  AND ph.POSN_DATE_INT = ?
  AND (
    UPPER(ph.DESCRIPTION) LIKE ?
    OR UPPER(ph.COMPANY_SYMBOL) LIKE ?
    OR UPPER(ph.SECURITY) LIKE ?
    OR UPPER(ph.BBG_TICKER) LIKE ?
  )
"""


def _compact_date(s: str) -> str:
    return s.replace("-", "")[:8]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--portfolio", required=True, help="PSC PORTFOLIO name (exact or prefix)")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--search", required=True, help="Keyword e.g. TIDEWATER, UAL, QQQ")
    parser.add_argument("--dsn", default="PSC_VIEWER")
    args = parser.parse_args()

    try:
        import pyodbc
    except ImportError:
        print("pyodbc required", file=sys.stderr)
        sys.exit(1)

    needle = f"%{args.search.upper()}%"
    date_compact = _compact_date(args.date)
    conn = pyodbc.connect(f"DSN={args.dsn}")
    try:
        cur = conn.cursor()
        cur.execute(
            PSC_LABEL_SQL,
            (f"{args.portfolio}%", date_compact, needle, needle, needle, needle),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"No PSC rows for portfolio={args.portfolio!r} date={date_compact} search={args.search!r}")
        return

    print(f"PSC rows: {len(rows)} (portfolio~{args.portfolio}, date={date_compact}, search={args.search})\n")
    for i, row in enumerate(rows, 1):
        rec = {cols[j]: (None if row[j] is None else str(row[j]).strip()) for j in range(len(cols))}
        print(f"--- Row {i} ---")
        for k, v in rec.items():
            if v:
                print(f"  {k}: {v}")
        disp = portfolio_details_display_ticker(
            security_type=rec.get("SECURITY_TYPE"),
            company_symbol=rec.get("COMPANY_SYMBOL"),
            description=rec.get("DESCRIPTION"),
            bbg_ticker=rec.get("BBG_TICKER"),
            security=rec.get("SECURITY"),
        )
        print(f"  [reconcile display ticker today]: {disp!r}")
        print(f"  bond_pattern(DESCRIPTION): {_looks_like_bond_description(rec.get('DESCRIPTION'))}")
        print(f"  bond_pattern(SECURITY): {_looks_like_bond_description(rec.get('SECURITY'))}")
        print(f"  bond_pattern(BBG): {_looks_like_bond_description(rec.get('BBG_TICKER'))}")
        print(f"  option_pattern(DESCRIPTION): {_looks_like_option_description(rec.get('DESCRIPTION'))}")
        print(f"  option_compact(BBG): {parse_option_contract_key(rec.get('BBG_TICKER'))}")
        print()

    print(
        "Diamond GetPortfolio (per position) label-related fields NOT in reconcile SQL today:\n"
        "  SecurityName, PricingTicker, PrimaryBBGID, CompositeBBGID,\n"
        "  UnderlyingSecurity, UnderlyingBBGID, SecurityID, SecurityParentID,\n"
        "  CUSIP, ISIN, SEDOL, Currency\n"
        "\n"
        "Portfolio Details *bond* display uses Bloomberg SECURITY_NAME keyed by BBG_TICKER,\n"
        "not PSC DESCRIPTION — reconcile does not call Bloomberg yet.\n"
    )


if __name__ == "__main__":
    main()
