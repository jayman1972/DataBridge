"""Unit checks for NAV checker prior-date holiday walk-back."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.nav_sheet_parse import (  # noqa: E402
    prior_business_day_iso,
    prior_business_days_for_lookup,
    prior_open_sheet_is_usable,
    previous_business_day_iso,
)


def test_may_19_holiday_prior_chain() -> None:
    assert prior_business_day_iso("2026-05-19") == "2026-05-18"
    assert previous_business_day_iso("2026-05-18") == "2026-05-15"
    days = prior_business_days_for_lookup("2026-05-18", max_days=5)
    assert days[0] == "2026-05-18"
    assert "2026-05-15" in days


def test_prior_sheet_usable() -> None:
    val = "2026-05-19"
    assert prior_open_sheet_is_usable("2026-05-15", val, "2026-05-18") == (True, None)
    assert prior_open_sheet_is_usable("2026-05-19", val, "2026-05-18")[0] is False
    assert prior_open_sheet_is_usable("2026-05-19", val, "2026-05-15")[0] is False
    assert prior_open_sheet_is_usable(None, val, "2026-05-18")[0] is False


if __name__ == "__main__":
    test_may_19_holiday_prior_chain()
    test_prior_sheet_usable()
    print("ok")
