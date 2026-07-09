"""Unit tests for close price reconciliation keys."""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sggg.close_price_reconcile import (
    _compute_dollar_difference,
    _diamond_match_key,
    aggregate_psc_by_security,
    align_diamond_bond_close,
    apply_diamond_price_discount,
    build_trade_index_by_match_key,
    compute_daily_pnl,
    diamond_futures_contract_multiplier,
    is_bond_like_position,
    is_cash_position,
    is_futures_like_position,
    merge_orphan_positions_by_close_and_notional,
    merge_positions_by_secondary_ids,
    normalize_bbg_key,
    normalize_diamond_close_price,
    normalize_diamond_futures_close,
    parse_futures_option_contract_key,
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


def test_futures_contract_key_nqu6_sep26() -> None:
    nq = "fut:NQ|2026-09"
    assert parse_futures_contract_key("NQU6 US") == nq
    assert parse_futures_contract_key("NQU6 Index") == nq
    assert (
        reconcile_match_key(
            bbg_ticker="NQU6 Index",
            security_type="Futures",
        )
        == nq
    )
    assert (
        reconcile_match_key(
            company_symbol="NASDAQ 100 E-MINI Sep26",
            security_name="NASDAQ 100 E-MINI Sep26",
            security_type="Futures",
        )
        == nq
    )
    assert (
        reconcile_match_key(
            company_symbol="NQU6 Index",
            description="NASDAQ 100 E-MINI Sep26",
            cusip="NQU6",
            security_type="Futures",
        )
        == nq
    )
    assert (
        _diamond_match_key(
            {
                "SecurityName": "NASDAQ 100 E-MINI Sep26",
                "SecurityType": "Futures",
                "PricingTicker": "NQU6 Index",
                "CUSIP": "NQU6",
            }
        )
        == nq
    )


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


def test_futures_match_nqu6_sep26_with_pseudo_cusip() -> None:
    """Diamond/PSC both store NQU6 as CUSIP — must still match on contract key."""
    nq = "fut:NQ|2026-09"
    psc_rows = [
        {
            "company_symbol": "NQU6 Index",
            "description": "NASDAQ 100 E-MINI Sep26",
            "bbg_ticker": "NQU6 Index",
            "security": "NQU6 Index",
            "isin": "",
            "cusip": "NQU6",
            "sedol": "",
            "security_type": "Futures",
            "long_short": "LONG",
            "quantity": 12,
            "close_price": 30313.75,
            "contract_size": 20.0,
            "underlying_contract_size": 20.0,
        },
    ]
    psc = aggregate_psc_by_security(psc_rows)
    dia_key = _diamond_match_key(
        {
            "SecurityName": "NASDAQ 100 E-MINI Sep26",
            "SecurityType": "Futures",
            "PricingTicker": "NQU6 Index",
            "CUSIP": "NQU6",
            "PortfolioPrice": 727530.0,
            "QuantityMultiplier": 20.0,
            "PreDiscountPrice": 30313.75,
            "Quantity": 12,
        }
    )
    assert dia_key == nq
    assert next(iter(psc.keys())) == dia_key


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


def test_alphadesk_dividends_require_matching_ex_date() -> None:
    rows = [
        {
            "company_symbol": "PXT.CA",
            "description": "PAREX RESOURCES INC",
            "bbg_ticker": "PXT CN Equity",
            "security": "PXT.CA",
            "isin": "",
            "cusip": "",
            "sedol": "B575D14",
            "security_type": "Stock",
            "long_short": "LONG",
            "quantity": 24500,
            "close_price": 12.34,
            "dividends": 7353.50,
            "posn_date_int": "20260630",
            "dividend_ex_date_int": "20260608",
        },
        {
            "company_symbol": "BDGI.CA",
            "description": "BADGER INFRASTRUCTURE SOLUTI",
            "bbg_ticker": "BDGI CN Equity",
            "security": "BDGI.CA",
            "isin": "",
            "cusip": "",
            "sedol": "BLCW7S7",
            "security_type": "Stock",
            "long_short": "LONG",
            "quantity": 0,
            "close_price": 40.00,
            "dividends": 1833.00,
            "posn_date_int": "20260630",
            "dividend_ex_date_int": "20260630",
        },
    ]

    agg = aggregate_psc_by_security(rows)

    pxt = agg["sedol:B575D14"]
    bdgi = agg["sedol:BLCW7S7"]
    assert pxt["alphadesk_dividends_present"] is False
    assert pxt["alphadesk_dividends"] == 0.0
    assert bdgi["alphadesk_dividends_present"] is True
    assert bdgi["alphadesk_dividends"] == 1833.00


def test_bond_daily_mtm_pnl_par_multiplier() -> None:
    """Bond MTM must use 0.01 mult when prices are normalized to par %."""
    pos_t = {
        "SecurityName": "Vermilion Energy Inc. 7.25% 15FEB2033",
        "SecurityType": None,
        "PricingTicker": "VETCN 7.25 02-33 CA",
        "PortfolioPrice": 0.9745,
        "Quantity": 200000.0,
        "LongShort": "Long",
        "LocalMarketValue": 194900.0,
        "BaseMarketValue": 194900.0,
    }
    pos_prior = {
        "SecurityName": "Vermilion Energy Inc. 7.25% 15FEB2033",
        "SecurityType": None,
        "PricingTicker": "VETCN 7.25 02-33 CA",
        "PortfolioPrice": 0.97395,
        "Quantity": 200000.0,
        "LongShort": "Long",
        "LocalMarketValue": 194790.0,
        "BaseMarketValue": 194790.0,
    }
    pnl = compute_daily_pnl(
        position_T=pos_t,
        position_prior=pos_prior,
        trades_for_symbol=[],
        bond_like=True,
    )
    assert pnl is not None
    assert abs(pnl - 110.0) < 1.0


def test_corp_bond_bbg_ticker_not_futures() -> None:
    """Bloomberg corp FIGI tickers (@bvn4) must not parse as futures month codes."""
    assert parse_futures_contract_key("US923725AE50@bvn4 corp") is None
    assert parse_futures_contract_key("US31575FAC05@bvn4 corp") is None
    assert not is_futures_like_position(
        security_type="Bond",
        bbg_ticker="US923725AE50@bvn4 corp",
        security="VETCN 7.25 02-33 CA",
        cusip="923725AE5",
    )
    assert is_bond_like_position(
        security_type="Bond",
        description="VETCN 7.25 02-33 CA",
        bbg_ticker="US923725AE50@bvn4 corp",
    )


def test_cusip_not_matched_as_futures_root() -> None:
    assert parse_futures_contract_key("836720AG7") is None


def test_psc_bond_aggregate_not_futures_like() -> None:
    psc_rows = [
        {
            "company_symbol": "VETCN",
            "description": "VERMILION ENERGY INC",
            "bbg_ticker": "US923725AE50@bvn4 corp",
            "security": "VETCN 7.25 02-33 CA",
            "isin": "US923725AE50",
            "cusip": "923725AE5",
            "sedol": "",
            "security_type": "Bond",
            "long_short": "LONG",
            "quantity": 200000,
            "close_price": 98.079,
            "day_profit": 1839.4,
        },
    ]
    psc = aggregate_psc_by_security(psc_rows)
    bucket = next(iter(psc.values()))
    assert bucket["is_bond_like"] is True
    assert bucket["is_futures_like"] is False


def test_nq_futures_option_strike_and_name_matching() -> None:
    """NQ index options: Bloomberg strike 26500 = AlphaDesk 2650."""
    expected = "futopt:NQU6|2026-09|P|2650"
    assert parse_futures_option_contract_key("NQU6P 09/18/26 P2650") == expected
    assert parse_futures_option_contract_key("NQU6P    26500") == expected
    assert (
        parse_futures_option_contract_key("Put NASDAQ 100 E-MINI Sep26P 26500")
        == expected
    )
    assert (
        reconcile_match_key(
            bbg_ticker="NQU6P    26500",
            security_name="Put NASDAQ 100 E-MINI Sep26P 26500 $26500 18SEP2026",
            cusip="NQU6P",
        )
        == expected
    )


def test_align_diamond_close_skips_futopt_par_scaling() -> None:
    assert (
        align_diamond_bond_close(4.51, 434.25, is_futures_option_like=True) == 4.51
    )


def test_futopt_not_bond_like() -> None:
    assert not is_bond_like_position(
        description="CLQ6C 08/16/26 C77.5",
        bbg_ticker="CLQ6C 08/16/26 C77.5",
        match_key="futopt:CLQ6|2026-08|C|77.5",
    )


def test_es_futures_trade_qty_scaled_by_contract_multiplier() -> None:
    """ES futures trades report Quantity in index-point units (contracts × 50)."""
    trade = {
        "SecurityName": "S&P500 EMINI FUT  Sep26",
        "Ticker": "ESU6",
        "CUSIP": "ESU6",
        "SecurityType": "Futures",
        "TradeType": "Buy",
        "Quantity": 1250.0,
        "QuantityMultiplier": 50.0,
        "LocalAmountPerShare": 7495.12,
        "LocalNetAmount": -468445.0,
        "BaseNetAmount": -663465.0,
        "TradeDate": "2026-07-08T00:00:00",
        "IsReversal": False,
        "IsCancel": False,
    }
    idx = build_trade_index_by_match_key([trade], "2026-07-08")
    key = "fut:ES|2026-09"
    assert key in idx
    assert idx[key][0]["qty"] == Decimal("25")


def test_futures_option_trade_qty_scaled_by_contract_multiplier() -> None:
    """Crude futures option trades report qty in barrels (contracts x 1000)."""
    from decimal import Decimal

    trade = {
        "SecurityName": "CRUDE OIL FUT OPT Aug26C 77.5",
        "Ticker": "CLQ6C    77.5 PIT",
        "CUSIP": "CLQ6C",
        "TradeType": "Buy",
        "Quantity": 18000.0,
        "QuantityMultiplier": 1000.0,
        "LocalAmountPerShare": 0.33491,
        "LocalNetAmount": -6028.38,
        "TradeDate": "2026-07-07T00:00:00",
        "IsReversal": False,
        "IsCancel": False,
    }
    idx = build_trade_index_by_match_key([trade], "2026-07-07")
    key = "futopt:CLQ6|2026-08|C|77.5"
    assert key in idx
    assert idx[key][0]["qty"] == Decimal("18")


def test_futures_option_close_out_mtm_pnl() -> None:
    """CLQ6C Jul 7 — trade leg must use 18 contracts, not 18000."""
    pos_t = {
        "SecurityName": "CRUDE OIL FUT OPT Aug26C 77.5",
        "PricingTicker": "CLQ6C    77.5 PIT",
        "PortfolioPrice": 0.3,
        "Quantity": 45.0,
        "LongShort": "Long",
        "LocalMarketValue": 13500.0,
        "BaseMarketValue": 19163.25,
        "QuantityMultiplier": 1000.0,
    }
    pos_prior = {
        "SecurityName": "CRUDE OIL FUT OPT Aug26C 77.5",
        "PricingTicker": "CLQ6C    77.5 PIT",
        "PortfolioPrice": 0.15,
        "Quantity": 27.0,
        "LongShort": "Long",
        "LocalMarketValue": 4050.0,
        "BaseMarketValue": 5748.98,
        "QuantityMultiplier": 1000.0,
    }
    trades = [
        {
            "SecurityName": "CRUDE OIL FUT OPT Aug26C 77.5",
            "Ticker": "CLQ6C    77.5 PIT",
            "CUSIP": "CLQ6C",
            "TradeType": "Buy",
            "Quantity": 18000.0,
            "QuantityMultiplier": 1000.0,
            "LocalAmountPerShare": 0.33491,
            "LocalNetAmount": -6028.38,
            "BaseNetAmount": -8557.28,
            "TradeDate": "2026-07-07T00:00:00",
            "IsReversal": False,
            "IsCancel": False,
        }
    ]
    trade_index = build_trade_index_by_match_key(trades, "2026-07-07")
    key = "futopt:CLQ6|2026-08|C|77.5"
    pnl = compute_daily_pnl(
        position_T=pos_t,
        position_prior=pos_prior,
        trades_for_symbol=trade_index[key],
        futures_option_like=True,
    )
    assert pnl is not None
    assert abs(pnl - 4859.04) < 50.0


def test_futures_option_match_clq6c_and_cou6c() -> None:
    expected_aug_call = "futopt:CLQ6|2026-08|C|77.5"
    expected_aug_put70 = "futopt:CLQ6|2026-08|P|70"
    expected_aug_put65 = "futopt:CLQ6|2026-08|P|65"
    expected_sep_call = "futopt:COU6|2026-09|C|82"
    expected_sep_put = "futopt:COU6|2026-09|P|70"

    assert (
        parse_futures_option_contract_key("CLQ6C 08/16/26 C77.5")
        == expected_aug_call
    )
    assert (
        reconcile_match_key(security="CLQ6C 08/16/26 C77.5", company_symbol="CLQ6C 08/16/26 C77.5")
        == expected_aug_call
    )
    assert (
        parse_futures_option_contract_key("CLQ6C    77.5 PIT")
        == expected_aug_call
    )
    assert (
        reconcile_match_key(
            bbg_ticker="CLQ6C    77.5 PIT",
            security_name="CRUDE OIL FUT OPT Aug26C 77.5",
            cusip="CLQ6C",
        )
        == expected_aug_call
    )
    assert (
        parse_futures_option_contract_key("CRUDE OIL FUT OPT Aug26C 77.5")
        == expected_aug_call
    )
    assert (
        parse_futures_option_contract_key("CLQ6P 08/16/26 P70")
        == expected_aug_put70
    )
    assert (
        parse_futures_option_contract_key("CRUDE OIL FUT OPT Aug26P 65")
        == expected_aug_put65
    )
    assert (
        parse_futures_option_contract_key("COU6C 09/28/26 C82")
        == expected_sep_call
    )
    assert (
        reconcile_match_key(
            bbg_ticker="COU6C    82",
            security_name="CRUDE OIL OPT IPE Sep26C 82",
            isin="GB00JZMNGY97",
            cusip="COU6C",
        )
        == expected_sep_call
    )
    assert (
        parse_futures_option_contract_key("CRUDE OIL OPT IPE Sep26P 70")
        == expected_sep_put
    )
    assert (
        _diamond_match_key(
            {
                "SecurityName": "CRUDE OIL OPT IPE Sep26C 82",
                "PricingTicker": "COU6C    82",
                "CUSIP": "COU6C",
                "ISIN": "GB00JZMNGY97",
            }
        )
        == expected_sep_call
    )


def test_cg_ca_daily_mtm_pnl() -> None:
    """EHP Alpha Strategies CG.CA Jul 6 2026 — MTM should match AlphaDesk DAY_PROFIT."""
    pos_t = {
        "SecurityName": "Centerra Gold Inc.",
        "SecurityType": "Common Stock",
        "PricingTicker": "CG CN",
        "PortfolioPrice": 23.5,
        "Quantity": 58500.0,
        "LongShort": "Long",
        "QuantityMultiplier": 1.0,
        "PriceMultiplier": 1.0,
        "LocalMarketValue": 1374750.0,
        "BaseMarketValue": 1374750.0,
        "BaseDividendIncomeSinceReferenceDate": 0.0,
    }
    pos_prior = {
        "SecurityName": "Centerra Gold Inc.",
        "SecurityType": "Common Stock",
        "PricingTicker": "CG CN",
        "PortfolioPrice": 24.41,
        "Quantity": 59100.0,
        "LongShort": "Long",
        "QuantityMultiplier": 1.0,
        "PriceMultiplier": 1.0,
        "LocalMarketValue": 1442631.0,
        "BaseMarketValue": 1442631.0,
        "BaseDividendIncomeSinceReferenceDate": 3773.0,
    }
    trades = [
        {
            "Ticker": "CG",
            "SecurityName": "Centerra Gold Inc.",
            "TradeDate": "2026-07-06T00:00:00",
            "TradeType": "Sell",
            "Quantity": -600.0,
            "LocalAmountPerShare": 23.6427,
            "LocalNetAmount": 14185.62,
            "BaseNetAmount": 14185.62,
            "IsReversal": False,
            "IsCancel": False,
        }
    ]
    trade_index = build_trade_index_by_match_key(trades, "2026-07-06")
    key = reconcile_match_key(company_symbol="CG", security_name="Centerra Gold Inc.")
    assert key is not None
    assert key in trade_index
    pnl = compute_daily_pnl(
        position_T=pos_t,
        position_prior=pos_prior,
        trades_for_symbol=trade_index[key],
    )
    assert pnl is not None
    assert abs(pnl - (-53695.38)) < 1.0


def test_option_trade_qty_converted_from_share_units() -> None:
    """Diamond option trades use share units (contracts x 100) in Quantity."""
    from decimal import Decimal

    trades = [
        {
            "SecurityName": "Put State Street SPDR S&P 500 ETF Trust $725 24JUL2026",
            "Ticker": "SPY 7 P725",
            "TradeType": "Buy",
            "Quantity": 20000.0,
            "LocalAmountPerShare": 2.04,
            "LocalNetAmount": -40800.0,
            "BaseNetAmount": -57913.8,
            "TradeDate": "2026-07-07T00:00:00",
            "IsReversal": False,
            "IsCancel": False,
        }
    ]
    idx = build_trade_index_by_match_key(trades, "2026-07-07")
    assert len(idx) == 1
    key = next(iter(idx))
    assert idx[key][0]["qty"] == Decimal("200")


def test_option_close_out_mtm_pnl() -> None:
    """Closing a short option to flat should not 100x the trade leg."""
    pos_t = {
        "SecurityName": "Put State Street SPDR S&P 500 ETF Trust $725 24JUL2026",
        "PricingTicker": "SPY US 07/24/26 P725",
        "PortfolioPrice": 1.69,
        "Quantity": 0.0,
        "LongShort": "Flat",
        "LocalMarketValue": 0.0,
        "BaseMarketValue": 0.0,
    }
    pos_prior = {
        "SecurityName": "Put State Street SPDR S&P 500 ETF Trust $725 24JUL2026",
        "PricingTicker": "SPY US 07/24/26 P725",
        "PortfolioPrice": 1.69,
        "Quantity": 200.0,
        "LongShort": "Short",
        "LocalMarketValue": -33800.0,
        "BaseMarketValue": -48019.66,
    }
    trades = [
        {
            "SecurityName": "Put State Street SPDR S&P 500 ETF Trust $725 24JUL2026",
            "Ticker": "SPY 7 P725",
            "TradeType": "Buy",
            "Quantity": 20000.0,
            "LocalAmountPerShare": 2.04,
            "LocalNetAmount": -40800.0,
            "BaseNetAmount": -57913.8,
            "TradeDate": "2026-07-07T00:00:00",
            "IsReversal": False,
            "IsCancel": False,
        }
    ]
    trade_index = build_trade_index_by_match_key(trades, "2026-07-07")
    trade_rows = next(iter(trade_index.values()))
    pnl = compute_daily_pnl(
        position_T=pos_t,
        position_prior=pos_prior,
        trades_for_symbol=trade_rows,
        option_like=True,
    )
    assert pnl is not None
    assert abs(pnl - (-9940.7)) < 25.0


def test_diamond_rows_for_valuation_date_per_security_quote_lag() -> None:
    """Jul 3 reference must not drop AMAT when its QuoteDate is Jul 2 but others are Jul 3."""
    from sggg.close_price_reconcile import (
        _diamond_rows_for_valuation_date,
        _index_diamond_positions_by_match_key,
    )

    records = [
        {
            "SecurityName": "Applied Materials Inc.",
            "SecurityType": "Common Stock",
            "CompanySymbol": "AMAT",
            "CUSIP": "038222105",
            "ISIN": "US0382221051",
            "PricingTicker": "AMAT US",
            "QuoteDate": "2026-07-02",
            "PortfolioPrice": 603.04,
            "Quantity": 1100.0,
            "LongShort": "Long",
        },
        {
            "SecurityName": "Centerra Gold Inc.",
            "SecurityType": "Common Stock",
            "CompanySymbol": "CG",
            "CUSIP": "152006102",
            "ISIN": "CA1520061021",
            "PricingTicker": "CG CN",
            "QuoteDate": "2026-07-03",
            "PortfolioPrice": 24.41,
            "Quantity": 59100.0,
            "LongShort": "Long",
        },
    ]
    selected = _diamond_rows_for_valuation_date(records, "2026-07-03")
    assert len(selected) == 2
    amat = next(r for r in selected if r["CUSIP"] == "038222105")
    assert amat["QuoteDate"] == "2026-07-02"
    assert amat["PortfolioPrice"] == 603.04

    indexed = _index_diamond_positions_by_match_key(records, "2026-07-03")
    assert "isin:US0382221051" in indexed
    assert indexed["isin:US0382221051"]["PortfolioPrice"] == 603.04


def test_cg_ca_mtm_lookup_keys_resolve_cusip_and_line_ticker() -> None:
    """ISIN canonical key must find Diamond snapshots (CUSIP) and trades (line:CG)."""
    from sggg.close_price_reconcile import (
        _collect_mtm_lookup_keys,
        _lookup_first,
        _lookup_trades_for_keys,
        build_trade_index_by_match_key,
        compute_daily_pnl,
    )

    canonical = "isin:CA1520061021"
    psc = {
        "isin": "CA1520061021",
        "cusip": "152006102",
        "company_symbol": "CG.CA",
        "security": "CG.CA",
    }
    dia = {
        "isin": "",
        "cusip": "152006102",
        "pricing_ticker": "CG CN",
        "company_symbol": "Centerra Gold Inc.",
        "security_name": "Centerra Gold Inc.",
    }
    keys = _collect_mtm_lookup_keys(canonical, psc, dia)
    assert "cusip:152006102" in keys
    assert "line:CG" in keys

    pos_t_by_key = {
        "cusip:152006102": {
            "PortfolioPrice": 22.98,
            "Quantity": 59400.0,
            "LongShort": "Long",
            "LocalMarketValue": 1365012.0,
            "BaseMarketValue": 1365012.0,
            "BaseDividendIncomeSinceReferenceDate": 0.0,
            "PricingTicker": "CG CN",
            "SecurityName": "Centerra Gold Inc.",
        }
    }
    pos_prior_by_key = {
        "cusip:152006102": {
            "PortfolioPrice": 23.5,
            "Quantity": 58500.0,
            "LongShort": "Long",
            "LocalMarketValue": 1374750.0,
            "BaseMarketValue": 1374750.0,
            "BaseDividendIncomeSinceReferenceDate": 3773.0,
            "PricingTicker": "CG CN",
            "SecurityName": "Centerra Gold Inc.",
        }
    }
    trades = [
        {
            "TradeDate": "2026-07-07T00:00:00",
            "TradeType": "Buy",
            "Quantity": 600.0,
            "LocalAmountPerShare": 23.354,
            "LocalNetAmount": -14012.4,
            "BaseNetAmount": -14012.4,
            "IsReversal": False,
            "IsCancel": False,
            "Ticker": "CG",
            "SecurityName": "Centerra Gold Inc.",
        },
        {
            "TradeDate": "2026-07-07T00:00:00",
            "TradeType": "Buy",
            "Quantity": 300.0,
            "LocalAmountPerShare": 23.35,
            "LocalNetAmount": -7005.0,
            "BaseNetAmount": -7005.0,
            "IsReversal": False,
            "IsCancel": False,
            "Ticker": "CG",
            "SecurityName": "Centerra Gold Inc.",
        },
    ]
    trade_index = build_trade_index_by_match_key(trades, "2026-07-07")

    pos_t = _lookup_first(pos_t_by_key, keys)
    pos_prior = _lookup_first(pos_prior_by_key, keys)
    trade_rows = _lookup_trades_for_keys(trade_index, keys)
    assert pos_t is not None
    assert pos_prior is not None
    assert len(trade_rows) == 2
    pnl = compute_daily_pnl(
        position_T=pos_t,
        position_prior=pos_prior,
        trades_for_symbol=trade_rows,
    )
    assert pnl is not None
    assert abs(pnl - (-30755.4)) < 1.0


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
    test_futures_contract_key_nqu6_sep26()
    test_futures_contract_key_nqm6()
    test_diamond_futures_close_notional_to_index()
    test_futures_match_nqu6_sep26_with_pseudo_cusip()
    test_futures_match_and_price_reconcile()
    test_futures_option_match_clq6c_and_cou6c()
    test_bond_daily_mtm_pnl_par_multiplier()
    test_corp_bond_bbg_ticker_not_futures()
    test_cusip_not_matched_as_futures_root()
    test_psc_bond_aggregate_not_futures_like()
    test_futopt_not_bond_like()
    test_nq_futures_option_strike_and_name_matching()
    test_align_diamond_close_skips_futopt_par_scaling()
    test_es_futures_trade_qty_scaled_by_contract_multiplier()
    test_futures_option_trade_qty_scaled_by_contract_multiplier()
    test_futures_option_close_out_mtm_pnl()
    test_aggregate_psc_net_shares()
    test_alphadesk_dividends_require_matching_ex_date()
    test_cg_ca_daily_mtm_pnl()
    test_option_trade_qty_converted_from_share_units()
    test_option_close_out_mtm_pnl()
    test_diamond_rows_for_valuation_date_per_security_quote_lag()
    test_cg_ca_mtm_lookup_keys_resolve_cusip_and_line_ticker()
    print("ok")
