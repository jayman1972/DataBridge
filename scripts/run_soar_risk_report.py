"""Generate the independent SOAR risk PDF (scheduled Mondays or on demand)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(ROOT))
    import data_bridge
    from sggg.soar_risk_math import parse_iso_date
    from sggg.soar_risk_service import (
        generate_soar_risk_report,
        save_soar_risk_report,
        scheduled_as_of,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Week-ending Friday (YYYY-MM-DD); default previous Friday")
    parser.add_argument("--output", type=Path, help="Write a review copy to this path")
    parser.add_argument("--save", action="store_true", help="Save to Reports - Internal")
    args = parser.parse_args()
    as_of = parse_iso_date(args.date) if args.date else scheduled_as_of()
    report, pdf = generate_soar_risk_report(
        as_of=as_of,
        bbg=data_bridge.bloomberg_client,
        supabase_client=data_bridge.supabase,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(pdf)
    position_date = parse_iso_date(report["position_date"])
    saved = save_soar_risk_report(position_date, pdf) if args.save else None
    print(
        json.dumps(
            {
                "position_date": report["position_date"],
                "fund_nav": report["fund_nav"],
                "class_b_navpu": report["class_b_navpu"],
                "var_95_1d": report["var_95_1d"],
                "var_observations": report["var_observations"],
                "pdf_bytes": len(pdf),
                "review_copy": str(args.output) if args.output else None,
                "saved_path": str(saved) if saved else None,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
