import unittest
from datetime import date

from bloomberg.merger_monitor import (
    build_ingest_row,
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


class BloombergMergerMonitorTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
