"""Unit tests for close price reconciliation keys."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (
    _compute_dollar_difference,
    _diamond_match_key,
    aggregate_psc_by_security,
    align_diamond_bond_close,
    apply_diamond_price_discount,
    diamond_futures_contract_multiplier,
    is_cash_position,
    merge_orphan_positions_by_close_and_notional,
    merge_positions_by_secondary_ids,
    normalize_bbg_key,
    normalize_diamond_close_price,
    normalize_diamond_futures_close,
    parse_futures_contract_key,
    parse_option_contract_key,
    parse_preferred_bbg_key,
    portfolio_details_display_ticker,
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
    assert (
        normalize_diamond_close_price(
            0.97381,
            security_name="LEG",
            security_type="Bond",
            is_bond_like=True,
        )
        == 97.381
    )
    assert normalize_diamond_close_price(78.5, security_name="AAPL US Equity") == 78.5


def test_sinking_bond_price_discount_gnfpso() -> None:
    """EHP Strategic Income GNFPSO — live Diamond 2026-05-21."""
    assert (
        normalize_diamond_close_price(
            0.718820291,
            security_name="Guara Norte Sarl 5.198% 15JUN2034",
            security_type="Bond",
            is_bond_like=True,
            price_discount=26.33,
            pre_discount_price=97.573,
        )
        == 97.573
    )
    # Diamond often omits SecurityType; must still scale fractional par before adding discount.
    assert (
        normalize_diamond_close_price(
            0.718267766,
            security_name="Guara Norte Sarl 5.198% 15JUN2034",
            price_discount=26.33,
            pre_discount_price=97.498,
        )
        == 97.498
    )
    assert apply_diamond_price_discount(
        27.048268,
        price_discount=26.33,
        pre_discount_price=27.048268,
        is_bond_like=True,
    ) == round(27.048268 + 26.33, 6)
    assert apply_diamond_price_discount(71.882, price_discount=26.33, is_bond_like=True) == round(
        71.882 + 26.33, 6
    )


def test_option_price_not_scaled() -> None:
    assert (
        normalize_diamond_close_price(
            1.55,
            security_type="EquityOption",
            bbg_ticker="SLV 06/18/26 C87 US",
            is_bond_like=True,
        )
        == 1.55
    )
    assert align_diamond_bond_close(8.45, 8.45, is_bond_like=True, is_option_like=True) == 8.45


def test_align_diamond_bond_close_when_matched_to_par() -> None:
    assert align_diamond_bond_close(0.97381, 97.381, is_bond_like=False) == 97.381


def test_cadusd_cash_excluded() -> None:
    assert is_cash_position(company_symbol="CADUSD")
    assert reconcile_match_key(company_symbol="CADUSD") is None


def test_portfolio_details_display_ticker() -> None:
    assert (
        portfolio_details_display_ticker(
            security_type="EquityOption",
            company_symbol="QQQ",
            security="QQQ 05/21/26 P702 US",
            bbg_ticker="QQQ US 05/21/26 P702 EQUITY",
        )
        == "QQQ 05/21/26 P702 US"
    )
    assert (
        portfolio_details_display_ticker(
            security_type="Bond",
            company_symbol="TDW",
            security="TDW 9.125 07-30 US",
            description="TIDEWATER INC",
        )
        == "TDW 9.125 07-30 US"
    )
    assert (
        portfolio_details_display_ticker(
            security_type="Stock",
            company_symbol="ABX",
            security="ABX.CA",
        )
        == "ABX.CA"
    )


def test_match_key_equity_line_ticker() -> None:
    assert reconcile_match_key(bbg_ticker="HYG US Equity") == "line:HYG"
    assert reconcile_match_key(company_symbol="PSNY.US") == "line:PSNY"
    assert reconcile_match_key(bbg_ticker="HYG US", sedol="B4PJP68") == "sedol:B4PJP68"


def test_preferred_bbg_match_key() -> None:
    bep = "pref:BEP.PR.S.CA"
    assert parse_preferred_bbg_key("BEP.PR.S.CA") == bep
    assert reconcile_match_key(company_symbol="BEP.PR.S.CA", security_type="Preferred") == bep
    assert reconcile_match_key(bbg_ticker="PWI.PR.A.CA") == "pref:PWI.PR.A.CA"


def test_orphan_merge_preferred_and_equity() -> None:
    psc = {
        "pref:BEP.PR.S.CA": {
            "match_key": "pref:BEP.PR.S.CA",
            "ticker": "BEP.PR.S.CA",
            "shares": 34000.0,
            "close_price": 25.0,
            "qty_multiplier": 1.0,
            "security_type": "Preferred",
            "is_preferred_like": True,
        },
        "line:PSNY": {
            "match_key": "line:PSNY",
            "ticker": "PSNY.US",
            "shares": 14.0,
            "close_price": 19.65,
            "qty_multiplier": 1.0,
            "security_type": "Stock",
        },
    }
    dia = {
        "line:BROOKFIELD RENEWABLE PARTNERS LP CLASS A PFD 5.75% - SERIES19": {
            "match_key": "line:BROOKFIELD RENEWABLE PARTNERS LP CLASS A PFD 5.75% - SERIES19",
            "ticker": "Brookfield Renewable Partners LP Class A PFD 5.75% - Series19",
            "shares": 34000.0,
            "close_price": 25.0,
            "qty_multiplier": 1.0,
            "security_type": "Preferred",
            "is_preferred_like": True,
        },
        "line:POLESTAR AUTOMOTIVE HOLDING UK PLC": {
            "match_key": "line:POLESTAR AUTOMOTIVE HOLDING UK PLC",
            "ticker": "Polestar Automotive Holding UK PLC",
            "shares": 14.0,
            "close_price": 19.65,
            "qty_multiplier": 1.0,
            "security_type": "Stock",
        },
    }
    psc_out, dia_out, n = merge_orphan_positions_by_close_and_notional(psc, dia)
    assert n == 2
    assert set(psc_out.keys()) == set(dia_out.keys())
    assert len(psc_out) == 2


def test_option_contract_key_cross_format() -> None:
    a = parse_option_contract_key("SPY 06/18/26 P675 US")
    b = parse_option_contract_key("SPY US 06/18/26 P675")
    assert a == b == "opt:SPY|2026-06-18|P|675"
    assert reconcile_match_key(company_symbol="SPY 06/18/26 P675 US") == a
    assert reconcile_match_key(company_symbol="SPY US 06/18/26 P702") != a


def test_option_security_field_and_long_description() -> None:
    hyg = "opt:HYG|2026-05-22|C|80"
    assert parse_option_contract_key("HYG 05/22/26 C80 US") == hyg
    assert (
        reconcile_match_key(
            company_symbol="HYG",
            description="Call iShares iBoxx USD High Yield Corporate Bond ETF $80 22MAY2026",
            security="HYG 05/22/26 C80 US",
            security_type="EquityOption",
            underlying_company_symbol="HYG",
        )
        == hyg
    )
    dram = "opt:DRAM|2026-05-22|P|51.5"
    assert parse_option_contract_key("DRAM 05/22/26 P51.5") == dram
    assert parse_option_contract_key("DRAM US 05/22/26 P51.5 EQUITY") == dram
    assert (
        parse_option_contract_key(
            "Put Roundhill Memory ETF $51.50 22MAY2026",
            underlying_root="DRAM",
        )
        == dram
    )
    assert (
        reconcile_match_key(
            company_symbol="DRAM",
            description="Put Roundhill Memory ETF $51.50 22MAY2026",
            security="DRAM 05/22/26 P51.5",
            security_type="EquityOption",
            bbg_ticker="DRAM US 05/22/26 P51.5 EQUITY",
        )
        == dram
    )


def test_diamond_underlying_index_hyg_call() -> None:
    from sggg.close_price_reconcile import build_underlying_ticker_index, _diamond_match_key

    records = [
        {
            "SecurityName": "iShares iBoxx $ High Yield Corporate Bond ETF",
            "PricingTicker": "HYG US",
            "CompositeBBGID": "BBG000R2T3H9",
            "ISIN": "US4642885135",
        },
        {
            "SecurityName": "Call iShares iBoxx USD High Yield Corporate Bond ETF $80 22MAY2026",
            "PricingTicker": "",
            "UnderlyingBBGID": "BBG000R2T3H9",
            "Quantity": 4000,
        },
    ]
    idx = build_underlying_ticker_index(records)
    assert idx.get("BBG000R2T3H9") == "HYG"
    key = _diamond_match_key(records[1], underlying_index=idx)
    assert key == "opt:HYG|2026-05-22|C|80"


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


def test_bond_dollar_diff_par_scaling() -> None:
    """475k face × (71.882 - 97.573) / 100 ≈ -122k, not -12.2M."""
    psc = {"shares": 475000.0, "close_price": 97.573, "qty_multiplier": 0.01}
    dia = {"shares": 475000.0, "close_price": 71.882, "qty_multiplier": 0.01}
    _, dollar_diff, _ = _compute_dollar_difference(psc, dia)
    assert dollar_diff == round(475000 * 0.01 * (71.882 - 97.573), 2)


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
            "description": "AAPL US Equity",
            "bbg_ticker": "AAPL US 01/17/2025 C150",
            "security": "AAPL US 01/17/2025 C150",
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
    assert "C150" in row["ticker"]
    psc = row
    dia = {"shares": 10.0, "close_price": 5.30, "qty_multiplier": 100.0}
    _, dollar_diff, _ = _compute_dollar_difference(psc, dia)
    assert dollar_diff == round(10 * 100 * (5.30 - 5.25), 2)


def test_futures_contract_key_nqm6() -> None:
    nq = "fut:NQ|2026-06"
    assert parse_futures_contract_key("NQM6 US") == nq
    assert parse_futures_contract_key("NQM6 Index") == nq
    assert (
        reconcile_match_key(
            bbg_ticker="NQM6 US",
            security_type="Futures",
        )
        == nq
    )
    assert (
        reconcile_match_key(
            company_symbol="NASDAQ 100 E-MINI Jun26",
            security_name="NASDAQ 100 E-MINI Jun26",
            security_type="Futures",
        )
        == nq
    )


def test_diamond_futures_close_notional_to_index() -> None:
    row = {
        "PortfolioPrice": 609765.0,
        "QuantityMultiplier": 20.0,
        "PreDiscountPrice": 0.0,
        "PriceMultiplier": 0.05,
    }
    assert normalize_diamond_futures_close(row) == 30488.25
    assert diamond_futures_contract_multiplier(row) == 20.0
    spi = {
        "PortfolioPrice": 153875.0,
        "PriceMultiplier": 0.04,
        "PreDiscountPrice": 6155.0,
        "QuantityMultiplier": 25.0,
    }
    assert normalize_diamond_futures_close(spi) == 6155.0


def test_futures_match_and_price_reconcile() -> None:
    psc_rows = [
        {
            "company_symbol": "NQM6",
            "description": "NASDAQ 100 E-MINI Jun26",
            "bbg_ticker": "NQM6 US",
            "security": "NQM6 US",
            "isin": "",
            "cusip": "",
            "sedol": "",
            "security_type": "Futures",
            "long_short": "LONG",
            "quantity": 28,
            "close_price": 30488.25,
            "contract_size": 20.0,
            "underlying_contract_size": 20.0,
        },
    ]
    psc = aggregate_psc_by_security(psc_rows)
    dia_key = _diamond_match_key(
        {
            "SecurityName": "NASDAQ 100 E-MINI Jun26",
            "SecurityType": "Futures",
            "PricingTicker": "NQM6 Index",
            "PortfolioPrice": 609765.0,
            "QuantityMultiplier": 20.0,
            "Quantity": 28,
        }
    )
    assert dia_key == "fut:NQ|2026-06"
    assert next(iter(psc.keys())) == dia_key
    psc_bucket = next(iter(psc.values()))
    _, dollar_diff, _ = _compute_dollar_difference(
        psc_bucket,
        {
            "shares": 28.0,
            "close_price": 30488.25,
            "qty_multiplier": 20.0,
        },
    )
    assert dollar_diff == 0.0


def test_aggregate_psc_net_shares() -> None:
    rows = [
        {
            "company_symbol": "HYG.US",
            "description": "HYG US Equity",
            "bbg_ticker": "HYG US Equity",
            "security": "HYG.US",
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
            "security": "HYG.US",
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
    test_bond_compact_match_key()
    test_bond_diamond_price_scaled()
    test_option_price_not_scaled()
    test_align_diamond_bond_close_when_matched_to_par()
    test_bond_dollar_diff_par_scaling()
    test_cadusd_cash_excluded()
    test_portfolio_details_display_ticker()
    test_match_key_equity_line_ticker()
    test_preferred_bbg_match_key()
    test_orphan_merge_preferred_and_equity()
    test_option_contract_key_cross_format()
    test_option_security_field_and_long_description()
    test_diamond_underlying_index_hyg_call()
    test_cash_excluded()
    test_secondary_id_merge()
    test_one_sided_alphadesk_only()
    test_one_sided_diamond_only()
    test_options_contract_multiplier()
    test_futures_contract_key_nqm6()
    test_diamond_futures_close_notional_to_index()
    test_futures_match_and_price_reconcile()
    test_aggregate_psc_net_shares()
    print("ok")
