"""
Query AlphaDesk PSC for boxed positions (long + short same security) on live dates.

Requires OpenVPN / PSC_VIEWER ODBC. Does not use Diamond.

Examples:
  python scripts/run_psc_boxed_live.py 2026-05-21
  python scripts/run_psc_boxed_live.py 2026-05-15 2026-05-20 2026-05-21
  python scripts/run_psc_boxed_live.py --from 2026-04-01 --to 2026-04-30
  python scripts/run_psc_boxed_live.py --fund alpha --fund tactical 2026-05-19
  python scripts/run_psc_boxed_live.py --paired 2026-05-21
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sggg.nav_sheet_parse import NAV_CHECKER_FUND_ID_TO_PSC, normalize_valuation_date
from sggg.psc_boxed_positions import (
    _security_key,
    _side,
    detect_boxed_positions,
    fetch_boxed_positions_for_funds,
    fetch_psc_positions_for_portfolio,
)

# Short names for --fund filter (case-insensitive)
FUND_CHOICES: List[Tuple[str, str, str]] = [
    ("415a3530-3034-4536-4432-303030364337", "alpha", "EHP Alpha"),
    ("41010000-7F7A-0A65-D559-45484608DB40", "tactical", "EHP Tact Growth Alt"),
    ("41323030-3031-4144-3637-303030364338", "select", "EHP Select Alt"),
    ("41010000-7F2A-D7E8-776F-45484608D91C", "strategic", "EHP Strat Inc Alt"),
    ("01010000-801A-4995-8370-45484608DE57", "exponential", "Expon Bal Grow Fund"),
]

_ALIAS_TO_SPEC: Dict[str, Dict[str, str]] = {}
for fid, alias, psc_name in FUND_CHOICES:
    _ALIAS_TO_SPEC[alias] = {"id": fid, "name": psc_name}
    _ALIAS_TO_SPEC[fid.lower()] = {"id": fid, "name": psc_name}


def _parse_iso(s: str) -> str:
    return normalize_valuation_date(s.strip())


def _date_range(from_iso: str, to_iso: str) -> List[str]:
    start = date.fromisoformat(from_iso)
    end = date.fromisoformat(to_iso)
    if end < start:
        start, end = end, start
    out: List[str] = []
    d = start
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _fund_specs(aliases: Optional[List[str]]) -> List[Dict[str, str]]:
    if not aliases:
        return [{"id": fid, "name": name} for fid, _alias, name in FUND_CHOICES]
    specs: List[Dict[str, str]] = []
    for raw in aliases:
        key = raw.strip().lower()
        spec = _ALIAS_TO_SPEC.get(key)
        if not spec:
            print(f"Unknown fund alias/id: {raw!r}", file=sys.stderr)
            print("Valid --fund values:", ", ".join(a for _, a, _ in FUND_CHOICES), file=sys.stderr)
            sys.exit(2)
        if spec not in specs:
            specs.append(spec)
    return specs


def _paired_securities(positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Long+short same security even when not classified as a box (same tags & account)."""
    by_key: Dict[str, List[Dict[str, Any]]] = {}
    for row in positions:
        key = _security_key(row)
        if key:
            by_key.setdefault(key, []).append(row)

    paired: List[Dict[str, Any]] = []
    for key, legs in by_key.items():
        long_qty = sum(abs(float(l.get("quantity") or 0)) for l in legs if _side(l.get("long_short")) == "long")
        short_qty = sum(abs(float(l.get("quantity") or 0)) for l in legs if _side(l.get("long_short")) == "short")
        if long_qty > 0.0001 and short_qty > 0.0001:
            sample = next((l for l in legs if _side(l.get("long_short")) == "long"), legs[0])
            paired.append(
                {
                    "security_key": key,
                    "company_symbol": sample.get("company_symbol"),
                    "description": sample.get("description") or sample.get("company_symbol"),
                    "long_quantity": long_qty,
                    "short_quantity": short_qty,
                }
            )
    paired.sort(key=lambda p: (p.get("description") or p.get("security_key") or "").upper())
    return paired


def _print_boxes(
    valuation_date: str,
    fund_specs: List[Dict[str, str]],
    boxed_by_fund: Dict[str, List[Dict[str, Any]]],
    positions_by_fund: Dict[str, List[Dict[str, Any]]],
    *,
    show_paired: bool,
    err: Optional[str],
) -> int:
    """Return count of boxes found across all funds for this date."""
    print(f"\n{'=' * 72}")
    print(f"Valuation date: {valuation_date}")
    if err:
        print(f"PSC error: {err}")
        return 0

    total_boxes = 0
    for spec in fund_specs:
        fid = spec["id"]
        name = spec.get("name") or fid
        portfolio = NAV_CHECKER_FUND_ID_TO_PSC.get(fid, "?")
        positions = positions_by_fund.get(fid) or []
        boxes = boxed_by_fund.get(fid) or []
        total_boxes += len(boxes)

        print(f"\n--- {name} ({portfolio}) ---")
        print(f"  PSC rows: {len(positions):,}  |  boxed (tag/account mismatch): {len(boxes)}")

        if show_paired:
            paired = _paired_securities(positions)
            hidden = len(paired) - len(boxes)
            print(f"  long+short same security (any tags/accounts): {len(paired)}")
            if hidden:
                print(
                    f"    ({hidden} paired leg(s) not shown as boxes — same strategy/trade group and account)"
                )
            for p in paired[:20]:
                print(
                    f"    · {p.get('description') or p['security_key']}: "
                    f"long {p['long_quantity']:,.4g} / short {p['short_quantity']:,.4g}"
                )
            if len(paired) > 20:
                print(f"    … and {len(paired) - 20} more")

        for box in boxes:
            label = (
                box.get("company_symbol")
                or box.get("description")
                or box.get("bbg_ticker")
                or box.get("security_key")
            )
            print(f"  BOX [{box.get('box_type')}]: {label}")
            print(f"       {box.get('box_type_label')}")
            print(
                f"       long {box.get('long_quantity'):,.4g}  |  short {box.get('short_quantity'):,.4g}"
            )
            for leg in (box.get("long_legs") or [])[:3]:
                print(
                    f"         L: qty={leg.get('quantity')} strat={leg.get('strategy')!r} "
                    f"tg={leg.get('trade_group')!r} acct={leg.get('account')!r}"
                )
            for leg in (box.get("short_legs") or [])[:3]:
                print(
                    f"         S: qty={leg.get('quantity')} strat={leg.get('strategy')!r} "
                    f"tg={leg.get('trade_group')!r} acct={leg.get('account')!r}"
                )

    if total_boxes == 0:
        print("\n  No boxed positions for this date (per AlphaDesk tag/account rules).")
    else:
        print(f"\n  Total boxes this date: {total_boxes}")
    return total_boxes


def main() -> None:
    parser = argparse.ArgumentParser(description="Live PSC boxed-position check for NAV checker funds.")
    parser.add_argument(
        "dates",
        nargs="*",
        help="Valuation date(s) YYYY-MM-DD (one or more)",
    )
    parser.add_argument("--from", dest="from_date", metavar="FROM", help="Start date (with --to)")
    parser.add_argument("--to", dest="to_date", metavar="TO", help="End date (with --from)")
    parser.add_argument(
        "--fund",
        action="append",
        dest="funds",
        metavar="ALIAS",
        help="Limit to fund(s): alpha, tactical, select, strategic, exponential",
    )
    parser.add_argument(
        "--dsn",
        default="PSC_VIEWER",
        help="ODBC DSN (default PSC_VIEWER)",
    )
    parser.add_argument(
        "--paired",
        action="store_true",
        help="Also list every long+short same-security pair (including non-boxed flat pairs)",
    )
    parser.add_argument(
        "--positions-only",
        action="store_true",
        help="Print row counts only (faster scan across many dates)",
    )
    args = parser.parse_args()

    dates: List[str] = []
    if args.from_date and args.to_date:
        dates = _date_range(_parse_iso(args.from_date), _parse_iso(args.to_date))
    elif args.dates:
        dates = [_parse_iso(d) for d in args.dates]
    else:
        dates = [date.today().isoformat()]

    fund_specs = _fund_specs(args.funds)
    print(f"Funds: {', '.join(s['name'] for s in fund_specs)}")
    print(f"DSN: {args.dsn}")
    print(f"Dates: {len(dates)} ({dates[0]} … {dates[-1]})" if len(dates) > 1 else f"Date: {dates[0]}")

    grand_total = 0
    dates_with_boxes: List[str] = []

    for valuation_date in dates:
        if args.positions_only:
            try:
                import pyodbc
            except ImportError:
                print("pyodbc not installed", file=sys.stderr)
                sys.exit(1)
            date_compact = valuation_date.replace("-", "")
            print(f"\n{valuation_date}:", end=" ")
            try:
                conn = pyodbc.connect(f"DSN={args.dsn}")
                cur = conn.cursor()
                parts = []
                for spec in fund_specs:
                    port = NAV_CHECKER_FUND_ID_TO_PSC.get(spec["id"])
                    if not port:
                        continue
                    n = len(fetch_psc_positions_for_portfolio(cur, port, date_compact))
                    parts.append(f"{spec['name'].split()[1] if ' ' in spec['name'] else spec['name']}={n}")
                conn.close()
                print("  ".join(parts) if parts else "(no portfolios)")
            except Exception as exc:
                print(f"ERROR {exc}")
            continue

        boxed_by_fund, positions_by_fund, err = fetch_boxed_positions_for_funds(
            fund_specs,
            valuation_date,
            store_portfolios=False,
            dsn=args.dsn,
        )
        n = _print_boxes(
            valuation_date,
            fund_specs,
            boxed_by_fund,
            positions_by_fund,
            show_paired=args.paired,
            err=err,
        )
        grand_total += n
        if n:
            dates_with_boxes.append(valuation_date)

    if len(dates) > 1:
        print(f"\n{'=' * 72}")
        print(f"Scanned {len(dates)} date(s). Total boxed rows: {grand_total}")
        if dates_with_boxes:
            print(f"Dates with at least one box: {', '.join(dates_with_boxes)}")
        else:
            print("No boxes on any scanned date (tag/account mismatch rule).")
            if not args.paired:
                print("Tip: re-run with --paired to see long+short pairs that share tags/accounts.")


if __name__ == "__main__":
    main()
