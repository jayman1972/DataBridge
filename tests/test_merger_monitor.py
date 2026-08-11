import unittest
from datetime import date

from bloomberg.merger_monitor import (
    build_ingest_row,
    derive_stock_consideration_fraction,
    payment_uses_stock,
    refresh_open_merger_actions,
    should_monitor_deal,
    ticker_classification,
)


class _Response:
    def __init__(self, data):
        self.data = data


class _TableQuery:
    def __init__(self, rows):
        self.rows = rows

    def select(self, _columns):
        return self

    def range(self, start, end):
        self.start = start
        self.end = end
        return self

    def execute(self):
        return _Response(self.rows[self.start:self.end + 1])


class _RpcQuery:
    def __init__(self, owner, params):
        self.owner = owner
        self.params = params

    def execute(self):
        self.owner.rpc_params.append(self.params)
        return _Response({"deals_upserted": len(self.params["p_rows"])})


class _Supabase:
    def __init__(self, rows):
        self.rows = rows
        self.rpc_params = []

    def table(self, name):
        if name != "security_merger_deals":
            raise AssertionError(name)
        return _TableQuery(self.rows)

    def rpc(self, name, params):
        if name != "ingest_security_merger_deals":
            raise AssertionError(name)
        return _RpcQuery(self, params)


class _Bloomberg:
    def get_reference_data(self, securities, _fields):
        return {
            security: {
                "CA_MA_DEAL_STATUS": "Pending",
                "CA_MA_PAYMENT_TYP": "Cash",
                "CA_MA_ACQUIRER_TICKER": "BUY US",
                "CA_MA_TARGET_TICKER": "TGT US",
            }
            for security in securities
        }


class _BloombergWithHistory:
    def __init__(self):
        self.history_calls = []

    def get_reference_data(self, securities, _fields):
        return {
            security: {
                "CA_MA_DEAL_STATUS": "Pending",
                "CA_MA_PAYMENT_TYP": "Cash and Stock",
                "CA_MA_CASH_TERMS": "10/sh.",
                "CA_MA_STOCK_TERMS": "0.5 Aqr sh./Tgt sh.",
                "CA_MA_ACQUIRER_TICKER": "BUY US Equity",
                "CA_MA_TARGET_TICKER": "TGT US Equity",
                "CA_MA_ANNOUNCED_DATE": "2026-07-20",
            }
            for security in securities
        }

    def get_historical_data(self, **kwargs):
        self.history_calls.append(kwargs)
        if kwargs["fields"] == ["PX_LAST"]:
            return [{"date": "2026-07-20", "PX_LAST": 40.0}]
        if kwargs["fields"] == ["CUR_MKT_CAP"]:
            currency = kwargs.get("currency")
            if currency == "CAD":
                return [{"date": "2026-07-20", "CUR_MKT_CAP": 6750.0}]
            if currency == "USD":
                return [{"date": "2026-07-20", "CUR_MKT_CAP": 5000.0}]
            raise AssertionError(currency)
        raise AssertionError(kwargs)


class _BloombergCanadianWithHistory(_BloombergWithHistory):
    def get_reference_data(self, securities, fields):
        rows = super().get_reference_data(securities, fields)
        for values in rows.values():
            values["CA_MA_ACQUIRER_TICKER"] = "BUY CN Equity"
        return rows


class BloombergMergerMonitorTests(unittest.TestCase):
    def test_stock_fraction_derivation_preserves_unknowns(self) -> None:
        self.assertEqual(
            derive_stock_consideration_fraction("Cash", None, "11.25/sh."),
            0.0,
        )
        self.assertEqual(
            derive_stock_consideration_fraction("Stock", None, None),
            1.0,
        )
        self.assertIsNone(
            derive_stock_consideration_fraction(
                "Cash or Stock",
                "0.5 Aqr sh./Tgt sh.",
                "10/sh.",
                40.0,
            )
        )

    def test_stock_fraction_derives_from_total_and_fixed_ratio_terms(self) -> None:
        self.assertAlmostEqual(
            derive_stock_consideration_fraction(
                "Cash and Stock",
                "USD 140 Mln",
                "510 Mln",
            ),
            140 / 650,
        )
        self.assertAlmostEqual(
            derive_stock_consideration_fraction(
                "Cash and Stock",
                "0.5 Aqr sh./Tgt sh.",
                "10/sh.",
                40.0,
                "USD",
            ),
            2 / 3,
        )
        self.assertIsNone(
            derive_stock_consideration_fraction(
                "Cash and Stock",
                "0.5 Aqr sh./Tgt sh.",
                "USD 10/sh.",
                40.0,
                "CAD",
            )
        )

    def test_payment_classification_is_tri_state(self) -> None:
        self.assertIs(payment_uses_stock("Cash", None), False)
        self.assertIs(payment_uses_stock("Cash and Stock", None), True)
        self.assertIs(
            payment_uses_stock(None, "0.1442 Aqr sh./Tgt sh."),
            True,
        )
        self.assertIsNone(payment_uses_stock(None, None))

    def test_multiple_acquirers_are_retained_and_classified(self) -> None:
        existing = {
            "action_id": "244404168",
            "announced_date": "2026-01-05",
            "target_is_private": False,
        }
        row = build_ingest_row(
            existing,
            {
                "CA_MA_ACQUIRER_TICKER": "AAA US Equity,BBB US Equity",
                "CA_MA_TARGET_TICKER": "TGT US Equity",
                "CA_MA_DEAL_STATUS": "Pending",
            },
        )
        self.assertEqual(row["acquirer_ticker_count"], 2)
        self.assertIs(row["acquirer_ticker_is_valid"], False)
        self.assertIs(row["eligible_for_model_builder"], True)

    def test_lifecycle_refresh_preserves_stock_funding_metrics(self) -> None:
        row = build_ingest_row(
            {
                "action_id": "244404168",
                "announced_date": "2026-01-05",
                "target_is_private": False,
                "deal_value_usd": 2_000_000_000,
                "deal_value_native": 2_000_000_000,
                "currency": "USD",
                "acquirer_market_cap_at_announcement_usd": 5_000_000_000,
                "acquirer_market_cap_at_announcement_native": 5_000_000_000,
                "stock_consideration_fraction": 0.6,
                "stock_consideration_value_usd": 1_200_000_000,
                "stock_consideration_value_native": 1_200_000_000,
                "stock_issuance_to_acquirer_market_cap": 0.24,
                "stock_consideration_calculation_method":
                    "stock_fraction_x_deal_value",
            },
            {
                "CA_MA_ACQUIRER_TICKER": "BUY US Equity",
                "CA_MA_TARGET_TICKER": "TGT US Equity",
                "CA_MA_DEAL_STATUS": "Pending",
            },
        )
        self.assertEqual(row["stock_consideration_fraction"], 0.6)
        self.assertEqual(row["stock_issuance_to_acquirer_market_cap"], 0.24)

    def test_completed_and_withdrawn_dates_are_mapped_by_status(self) -> None:
        existing = {
            "action_id": "244404168",
            "announced_date": "2026-01-05",
            "target_is_private": False,
        }
        completed = build_ingest_row(
            existing,
            {
                "CA_MA_DEAL_STATUS": "Completed",
                "CA_MA_COMPLETE_DT": "2026-07-20",
            },
        )
        self.assertEqual(completed["actual_completion_date"], "2026-07-20")
        self.assertNotIn("withdrawal_date", completed)

        withdrawn = build_ingest_row(
            existing,
            {
                "CA_MA_DEAL_STATUS": "Withdrawn",
                "CA_MA_COMPLETE_DT": "2026-07-21",
            },
        )
        self.assertEqual(withdrawn["withdrawal_date"], "2026-07-21")
        self.assertNotIn("actual_completion_date", withdrawn)

    def test_pre_announcement_lifecycle_dates_are_treated_as_unknown(self) -> None:
        row = build_ingest_row(
            {
                "action_id": "84188222",
                "announced_date": "2013-11-13",
                "target_is_private": False,
            },
            {
                "CA_MA_DEAL_STATUS": "Completed",
                "CA_MA_EXPECTED_COMPLETION_DATE": "2013-11-12",
                "CA_MA_COMPLETE_DT": "2013-11-12",
            },
        )

        self.assertNotIn("expected_completion_date", row)
        self.assertNotIn("actual_completion_date", row)

    def test_terminal_deals_are_rechecked_for_five_days(self) -> None:
        self.assertTrue(
            should_monitor_deal(
                {
                    "status": "Completed",
                    "actual_completion_date": "2026-07-23",
                },
                date(2026, 7, 26),
                5,
            )
        )
        self.assertFalse(
            should_monitor_deal(
                {
                    "status": "Completed",
                    "actual_completion_date": "2026-06-01",
                },
                date(2026, 7, 26),
                5,
            )
        )

    def test_ticker_classification(self) -> None:
        self.assertEqual(ticker_classification("AAA US Equity"), (1, True))
        self.assertEqual(ticker_classification("AAA US"), (1, True))
        self.assertEqual(
            ticker_classification("AAA US Equity,BBB US Equity"),
            (2, False),
        )
        self.assertEqual(ticker_classification(None), (0, False))

    def test_refresh_writes_public_and_unknown_targets_but_not_private(self) -> None:
        supabase = _Supabase([
            {
                "action_id": "100",
                "announced_date": "2026-07-20",
                "status": "Pending",
                "target_is_private": False,
            },
            {
                "action_id": "101",
                "announced_date": "2026-07-20",
                "status": "Pending",
                "target_is_private": None,
            },
            {
                "action_id": "102",
                "announced_date": "2026-07-20",
                "status": "Pending",
                "target_is_private": True,
            },
        ])
        result = refresh_open_merger_actions(
            supabase,
            _Bloomberg(),
            date(2026, 7, 26),
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["actions_requested"], 2)
        self.assertEqual(result["actions_refreshed"], 2)
        self.assertEqual(len(supabase.rpc_params), 1)
        written = supabase.rpc_params[0]["p_rows"]
        self.assertEqual([row["action_id"] for row in written], ["100", "101"])
        self.assertIs(written[0]["eligible_for_model_builder"], True)
        self.assertIs(written[1]["eligible_for_model_builder"], False)
        self.assertIs(written[0]["uses_stock_consideration"], False)

    def test_refresh_enriches_stock_funding_with_announcement_history(self) -> None:
        supabase = _Supabase([{
            "action_id": "200",
            "announced_date": "2026-07-20",
            "status": "Pending",
            "target_is_private": False,
            "deal_value_usd": 2_000_000_000,
            "currency": "USD",
        }])
        bloomberg = _BloombergWithHistory()
        result = refresh_open_merger_actions(
            supabase,
            bloomberg,
            date(2026, 7, 26),
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["stock_issuance_ratio_known"], 1)
        written = supabase.rpc_params[0]["p_rows"][0]
        self.assertAlmostEqual(written["stock_consideration_fraction"], 2 / 3)
        self.assertEqual(
            written["acquirer_market_cap_at_announcement_usd"],
            5_000_000_000,
        )
        self.assertEqual(
            written["acquirer_market_cap_at_announcement_native"],
            5_000_000_000,
        )
        self.assertAlmostEqual(
            written["stock_issuance_to_acquirer_market_cap"],
            (2_000_000_000 * (2 / 3)) / 5_000_000_000,
        )
        self.assertEqual(
            written["stock_consideration_calculation_method"],
            "stock_fraction_x_deal_value",
        )

    def test_refresh_preserves_cad_and_derives_matching_usd_values(self) -> None:
        supabase = _Supabase([{
            "action_id": "201",
            "announced_date": "2026-07-20",
            "status": "Pending",
            "target_is_private": False,
            "currency": "CAD",
            "deal_value_native": 2_700_000_000,
            # These values mirror the old USD-only schema and must be repaired.
            "acquirer_market_cap_at_announcement_native": 5_000_000_000,
            "stock_consideration_fraction": 2 / 3,
            "stock_consideration_value_native": 4_000_000_000 / 3,
            "stock_consideration_calculation_method":
                "stock_fraction_x_deal_value",
        }])
        bloomberg = _BloombergWithHistory()
        result = refresh_open_merger_actions(
            supabase,
            bloomberg,
            date(2026, 7, 26),
        )

        self.assertTrue(result["success"])
        written = supabase.rpc_params[0]["p_rows"][0]
        self.assertEqual(written["currency"], "CAD")
        self.assertEqual(written["deal_value_native"], 2_700_000_000)
        self.assertAlmostEqual(written["deal_value_usd"], 2_000_000_000)
        self.assertEqual(
            written["acquirer_market_cap_at_announcement_native"],
            6_750_000_000,
        )
        self.assertEqual(
            written["acquirer_market_cap_at_announcement_usd"],
            5_000_000_000,
        )
        self.assertAlmostEqual(
            written["stock_consideration_value_native"],
            1_800_000_000,
        )
        self.assertAlmostEqual(
            written["stock_consideration_value_usd"],
            4_000_000_000 / 3,
        )
        self.assertAlmostEqual(
            written["stock_issuance_to_acquirer_market_cap"],
            (2_700_000_000 * (2 / 3)) / 6_750_000_000,
        )
        history_currencies = {
            call.get("currency")
            for call in bloomberg.history_calls
            if call["fields"] == ["CUR_MKT_CAP"]
        }
        self.assertEqual(history_currencies, {"CAD", "USD"})

    def test_refresh_repairs_legacy_usd_mirror_for_canadian_acquirer(self) -> None:
        supabase = _Supabase([{
            "action_id": "202",
            "announced_date": "2026-07-20",
            "status": "Pending",
            "target_is_private": False,
            "currency": "USD",
            "deal_value_native": 2_000_000_000,
            "deal_value_usd": 2_000_000_000,
            "acquirer_market_cap_at_announcement_native": 5_000_000_000,
            "acquirer_market_cap_at_announcement_usd": 5_000_000_000,
        }])
        bloomberg = _BloombergCanadianWithHistory()
        result = refresh_open_merger_actions(
            supabase,
            bloomberg,
            date(2026, 7, 26),
        )

        self.assertTrue(result["success"])
        written = supabase.rpc_params[0]["p_rows"][0]
        self.assertEqual(written["currency"], "CAD")
        self.assertAlmostEqual(written["deal_value_native"], 2_700_000_000)
        self.assertAlmostEqual(written["deal_value_usd"], 2_000_000_000)
        self.assertEqual(
            written["acquirer_market_cap_at_announcement_native"],
            6_750_000_000,
        )


if __name__ == "__main__":
    unittest.main()
