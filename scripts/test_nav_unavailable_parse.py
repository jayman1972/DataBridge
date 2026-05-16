"""Smoke test for Diamond NAV-not-finalized error parsing."""
from sggg.nav_sheet_parse import nav_unavailable_user_message, parse_diamond_nav_unavailable

err = RuntimeError(
    'Diamond request failed: GetNAVSheet/ HTTP 400: {"Message":"The requested Valuation Period '
    '(End Date: 2026-05-19) has not yet been finalized..."}'
)
parsed = parse_diamond_nav_unavailable(err, "2026-05-16")
assert parsed is not None
assert parsed["end_date"] == "2026-05-19"
assert parsed["message"] == nav_unavailable_user_message("2026-05-19")
assert parse_diamond_nav_unavailable(RuntimeError("HTTP 500"), "2026-05-16") is None
print("ok")
