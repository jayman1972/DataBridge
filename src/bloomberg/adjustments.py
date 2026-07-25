"""Explicit Bloomberg historical adjustment profiles."""

from typing import Any, Dict, Optional, Tuple


ADJUSTMENT_FLAG_FIELDS = (
    "adjustment_follow_dpdf",
    "adjustment_normal",
    "adjustment_abnormal",
    "adjustment_split",
)


def normalize_historical_adjustment_profile(value: Any) -> Optional[str]:
    """Return a normalized RAW/CAPITAL/FULL profile or raise for invalid input."""
    if value is None or str(value).strip() == "":
        return None
    profile = str(value).strip().lower()
    if profile not in ("raw", "capital", "full"):
        raise ValueError("adjustment_profile must be raw, capital, or full")
    return profile


def historical_adjustment_settings(profile: str) -> Dict[str, bool]:
    """Map a normalized profile to Bloomberg HistoricalDataRequest settings."""
    normalized = normalize_historical_adjustment_profile(profile)
    if normalized is None:
        raise ValueError("adjustment_profile is required")
    return {
        "adjustmentFollowDPDF": False,
        "adjustmentNormal": normalized == "full",
        "adjustmentAbnormal": normalized == "full",
        "adjustmentSplit": normalized != "raw",
    }


def validate_historical_adjustment_contract(
    data: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """Validate the HTTP contract and return its normalized profile and error."""
    explicit_flags = {
        field_name: data[field_name]
        for field_name in ADJUSTMENT_FLAG_FIELDS
        if field_name in data
    }
    try:
        profile = normalize_historical_adjustment_profile(
            data.get("adjustment_profile")
        )
    except ValueError as exc:
        return None, str(exc)

    if profile is None:
        if explicit_flags:
            return None, (
                "adjustment_profile is required when explicit adjustment flags are supplied"
            )
        return None, None

    expected = {
        "adjustment_follow_dpdf": False,
        "adjustment_normal": profile == "full",
        "adjustment_abnormal": profile == "full",
        "adjustment_split": profile != "raw",
    }
    for field_name, value in explicit_flags.items():
        if not isinstance(value, bool):
            return None, f"{field_name} must be a boolean"
        if value != expected[field_name]:
            return None, (
                f"{field_name}={value!r} conflicts with "
                f"adjustment_profile={profile!r}"
            )
    return profile, None
