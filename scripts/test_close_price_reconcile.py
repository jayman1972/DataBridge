"""Unit tests for close price reconciliation keys."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (
    _compute_dollar_difference,
    aggregate_psc_by_security,
    display_ticker,
    normalize_bbg_key,
    normalize_diamond_close_price,
    reconcile_match_key,
)


def test_bbg_key_normalization() -> None:
    assert normalize_bbg_key("HYG US Equity") == "HYG US"
    assert normalize_bbg_key("hyg us") == "HYG US"


def test_bond_description_match_key() -> None:
    bond = "AAL 5 3/4 04/20/29"
    assert reconcile_match_key(description=bond, security_type="Bond") == f"desc:{bond.upper()}"
    assert (
        reconcile_match_key(
            description="AAL 5 3/4 04/20/29",
            security_name="AAL 5 3/4 04/20/29",
        )
        == reconcile_match_key(description=bond, security_type="Corporate Bond")
    )


def test_bond_diamond_price_scaled() -> None:
    assert normalize_diamond_close_price(0.99982, security_name="AAL 5 3/4 04/20/29") == 99.982
    assert normalize_diamond_close_price(78.5, security_name="AAPL US Equity") == 78.5


def test_match_key_bbg_equity() -> None:
    assert reconcile_match_key(bbg_ticker="HYG US Equity") == "bbg:HYG US"
    assert reconcile_match_key(bbg_ticker="HYG US", sedol="B4PJP68") == "bbg:HYG US"


def test_display_ticker_bond_uses_description() -> None:
    assert (
        display_ticker(
            company_symbol="AAL",
            description="AAL 5 3/4 04/20/29",
            security_type="Bond",
        )
        == "AAL 5 3/4 04/20/29"
    )


def test_one_sided_alphadesk_only() -> None:
    psc = {"shares": 1000.0, "close_price": 99.982, "qty_multiplier": 1.0}
    price_diff, dollar_diff, _ = _compute_dollar_difference(psc, None)
    assert price_diff is None
    assert dollar_diff == round(1000 * 99.982, 2)


def test_one_sided_diamond_only() -> None:
    dia = {"shares": 50.0, "close_price": 99.982, "qty_multiplier": 1.0}
    price_diff, dollar_diff, _ = _compute_dollar_difference(None, dia)
    assert price_diff is None
    assert dollar_diff == round(50 * 99.982, 2)


def test_options_contract_multiplier() -> None:
    rows = [
        {
            "company_symbol": "AAPL",
            "description": "AAPL US 01/17/2025 C150",
            "bbg_ticker": "AAPL US Equity",
            "isin": "",
            "cusip": "",
            "sedol": "",
            "security_type": "EquityOption",
            "long_short": "LONG",
            "quantity": 10,
            "close_price": 5.25,
        },
    ]
    agg = aggregate_psc_by_security(rows)
    row = next(iter(agg.values()))
    assert row["qty_multiplier"] == 100.0
    psc = row
    dia = {"shares": 10.0, "close_price": 5.30, "qty_multiplier": 100.0}
    _, dollar_diff, _ = _compute_dollar_difference(psc, dia)
    assert dollar_diff == round(10 * 100 * (5.30 - 5.25), 2)


def test_aggregate_psc_net_shares() -> None:
    rows = [
        {
            "company_symbol": "HYG.US",
            "description": "HYG US Equity",
            "bbg_ticker": "HYG US Equity",
            "isin": "US4642885135",
            "cusip": "",
            "sedol": "B4PJP68",
            "security_type": "Stock",
            "long_short": "LONG",
            "quantity": 48000,
            "close_price": 78.5,
        },
        {
            "company_symbol": "HYG.US",
            "description": "HYG US Equity",
            "bbg_ticker": "HYG US Equity",
            "isin": "US4642885135",
            "cusip": "",
            "sedol": "B4PJP68",
            "security_type": "Stock",
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
    test_bond_description_match_key()
    test_bond_diamond_price_scaled()
    test_match_key_bbg_equity()
    test_display_ticker_bond_uses_description()
    test_one_sided_alphadesk_only()
    test_one_sided_diamond_only()
    test_options_contract_multiplier()
    test_aggregate_psc_net_shares()
    print("ok")
