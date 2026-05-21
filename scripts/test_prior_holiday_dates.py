"""Unit checks for NAV checker prior-date holiday walk-back."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.nav_sheet_parse import (  # noqa: E402
    _display_class_label,
    _infer_fund_series_prefix,
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


def test_suffix_display_class_prefix() -> None:
    alpha_codes = ["200A", "200F", "200I", "200O", "UA", "UO"]
    prefix = _infer_fund_series_prefix(alpha_codes)
    assert prefix == "200"
    assert _display_class_label("UA", "", prefix) == "200UA"
    assert _display_class_label("UO", "", prefix) == "200UO"
    assert _display_class_label("200A", "", prefix) == "200A"
    assert _display_class_label("550UF", "", "550") == "550UF"

    sia_codes = ["800A", "800F", "800I", "800O", "FD", "UF"]
    prefix_800 = _infer_fund_series_prefix(sia_codes)
    assert prefix_800 == "800"
    assert _display_class_label("FD", "", prefix_800) == "800FD"
    assert _display_class_label("UF", "", prefix_800) == "800UF"


if __name__ == "__main__":
    test_may_19_holiday_prior_chain()
    test_prior_sheet_usable()
    test_suffix_display_class_prefix()
    print("ok")
