"""Unit tests for close price reconciliation keys."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (
    _compute_dollar_difference,
    aggregate_psc_by_security,
    align_diamond_bond_close,
    format_equity_display_ticker,
    is_cash_position,
    merge_positions_by_secondary_ids,
    normalize_bbg_key,
    normalize_diamond_close_price,
    parse_option_contract_key,
    portfolio_line_ticker,
    reconcile_match_key,
)


def test_bbg_key_normalization() -> None:
    assert normalize_bbg_key("HYG US Equity") == "HYG US"
    assert normalize_bbg_key("hyg us") == "HYG US"


def test_bond_compact_match_key() -> None:
    bond = "AAL 5 3/4 04/20/29"
    assert reconcile_match_key(company_symbol=bond, security_type="Bond") == f"bond:{bond.upper()}"
    assert reconcile_match_key(
        company_symbol="AAL 5 3/4 04/20/29",
        security_name="AAL 5 3/4 04/20/29",
    ) == reconcile_match_key(description=bond, security_type="Corporate Bond")


def test_bond_diamond_price_scaled() -> None:
    assert normalize_diamond_close_price(0.99982, security_name="AAL 5 3/4 04/20/29") == 99.982
    assert normalize_diamond_close_price(0.97381, security_name="LEG") == 97.381
    assert normalize_diamond_close_price(78.5, security_name="AAPL US Equity") == 78.5


def test_align_diamond_bond_close_when_matched_to_par() -> None:
    assert align_diamond_bond_close(0.97381, 97.381, is_bond_like=False) == 97.381


def test_cadusd_cash_excluded() -> None:
    assert is_cash_position(company_symbol="CADUSD")
    assert reconcile_match_key(company_symbol="CADUSD") is None


def test_equity_ticker_us_cn_suffix() -> None:
    assert format_equity_display_ticker(company_symbol="AMAT.US") == "AMAT US"
    assert format_equity_display_ticker(company_symbol="SHOP", currency="CAD") == "SHOP CN"
    assert (
        portfolio_line_ticker(company_symbol="HYG.US", bbg_ticker="HYG US Equity")
        == "HYG US"
    )


def test_match_key_equity_line_ticker() -> None:
    assert reconcile_match_key(bbg_ticker="HYG US Equity") == "line:HYG US"
    assert reconcile_match_key(bbg_ticker="HYG US", sedol="B4PJP68") == "sedol:B4PJP68"


def test_portfolio_line_ticker_bond_uses_company_symbol() -> None:
    assert (
        portfolio_line_ticker(
            company_symbol="AAL 5 3/4 04/20/29",
            description="AMERICAN AIRLINES GROUP INC",
            security_type="Bond",
        )
        == "AAL 5 3/4 04/20/29"
    )


def test_option_contract_key_cross_format() -> None:
    a = parse_option_contract_key("SPY 06/18/26 P675 US")
    b = parse_option_contract_key("SPY US 06/18/26 P675")
    assert a == b == "opt:SPY|2026-06-18|P|675"
    assert reconcile_match_key(company_symbol="SPY 06/18/26 P675 US") == a
    assert reconcile_match_key(company_symbol="SPY US 06/18/26 P702") != a


def test_cash_excluded() -> None:
    assert is_cash_position(company_symbol="CASH USD")
    assert reconcile_match_key(company_symbol="CASH USD") is None
    rows = [
        {
            "company_symbol": "CASH USD",
            "description": "Cash USD",
            "bbg_ticker": "",
            "isin": "",
            "cusip": "",
            "sedol": "",
            "security_type": "Cash",
            "long_short": "LONG",
            "quantity": 100,
            "close_price": 1.0,
        },
    ]
    assert aggregate_psc_by_security(rows) == {}


def test_secondary_id_merge() -> None:
    psc = {
        "line:HYG.US": {
            "match_key": "line:HYG.US",
            "ticker": "HYG.US",
            "shares": 100.0,
            "close_price": 78.5,
            "qty_multiplier": 1.0,
            "isin": "US4642885135",
        },
    }
    dia = {
        "line:HYG US": {
            "match_key": "line:HYG US",
            "ticker": "HYG US",
            "shares": 100.0,
            "close_price": 78.5,
            "qty_multiplier": 1.0,
            "isin": "US4642885135",
        },
    }
    psc_out, dia_out, n = merge_positions_by_secondary_ids(psc, dia)
    assert n == 1
    assert set(psc_out.keys()) == set(dia_out.keys())


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
            "company_symbol": "AAPL US 01/17/2025 C150",
            "description": "AAPL US Equity",
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
    assert row["ticker"] == "HYG US"
    assert row["shares"] == 45000.0


if __name__ == "__main__":
    test_bbg_key_normalization()
    test_bond_compact_match_key()
    test_bond_diamond_price_scaled()
    test_align_diamond_bond_close_when_matched_to_par()
    test_cadusd_cash_excluded()
    test_equity_ticker_us_cn_suffix()
    test_match_key_equity_line_ticker()
    test_portfolio_line_ticker_bond_uses_company_symbol()
    test_option_contract_key_cross_format()
    test_cash_excluded()
    test_secondary_id_merge()
    test_one_sided_alphadesk_only()
    test_one_sided_diamond_only()
    test_options_contract_multiplier()
    test_aggregate_psc_net_shares()
    print("ok")
