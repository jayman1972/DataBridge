"""Unit tests for close price reconciliation keys."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (
    aggregate_psc_by_security,
    display_ticker,
    normalize_bbg_key,
    reconcile_match_key,
)


def test_bbg_key_normalization() -> None:
    assert normalize_bbg_key("HYG US Equity") == "HYG US"
    assert normalize_bbg_key("hyg us") == "HYG US"


def test_match_key_bbg() -> None:
    assert reconcile_match_key(bbg_ticker="HYG US Equity") == "bbg:HYG US"
    assert reconcile_match_key(bbg_ticker="HYG US", sedol="B4PJP68") == "bbg:HYG US"


def test_match_key_sedol_fallback() -> None:
    assert reconcile_match_key(sedol="B4PJP68") == "sedol:B4PJP68"


def test_display_ticker_prefers_company_symbol() -> None:
    assert display_ticker(company_symbol="HYG.US", bbg_ticker="HYG US Equity") == "HYG.US"


def test_aggregate_psc_net_shares() -> None:
    rows = [
        {
            "company_symbol": "HYG.US",
            "bbg_ticker": "HYG US Equity",
            "isin": "US4642885135",
            "cusip": "",
            "sedol": "B4PJP68",
            "long_short": "LONG",
            "quantity": 48000,
            "close_price": 78.5,
        },
        {
            "company_symbol": "HYG.US",
            "bbg_ticker": "HYG US Equity",
            "isin": "US4642885135",
            "cusip": "",
            "sedol": "B4PJP68",
            "long_short": "SHORT",
            "quantity": 3000,
            "close_price": 78.5,
        },
    ]
    agg = aggregate_psc_by_security(rows)
    assert len(agg) == 1
    row = next(iter(agg.values()))
    assert row["ticker"] == "HYG.US"
    assert row["shares"] == 45000.0


if __name__ == "__main__":
    test_bbg_key_normalization()
    test_match_key_bbg()
    test_match_key_sedol_fallback()
    test_display_ticker_prefers_company_symbol()
    test_aggregate_psc_net_shares()
    print("ok")
