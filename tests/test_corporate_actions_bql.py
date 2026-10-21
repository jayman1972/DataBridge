from __future__ import annotations

import unittest
from datetime import datetime, timezone

from bloomberg.corporate_actions_bql import (
    PendingBql,
    build_formula,
    parse_table,
    validate_bql_symbols,
    validate_job_options,
)


def options(**changes):
    return validate_job_options(
        {
            "securities": ["META US Equity"],
            "scope": "security",
            "date_basis": "effective",
            "action_types": ["ticker_changes"],
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            **changes,
        }
    )


def table():
    return [
        [
            "ID",
            "DATES",
            "EFFECTIVE_DATE",
            "ANNOUNCED_DATE",
            "NEW_TICKER",
            "OLD_TICKER",
            "ticker_change(effective_date=range(2022-01-01,2022-12-31))",
        ],
        [
            "META US Equity",
            46316,
            44721,
            44497,
            "META US Equity",
            "FB US Equity",
            "META US Equity",
        ],
    ]


class BqlTests(unittest.TestCase):
    def test_live_meta_fixture_uses_event_dates_not_observation_date(self):
        events, warnings = parse_table(
            table(), "ticker_changes", options(), {"META US Equity": ["FB US Equity"]}
        )
        self.assertFalse(warnings)
        self.assertEqual(events[0]["date"], "2022-06-09")
        self.assertEqual(events[0]["announced_date"], "2021-10-28")
        self.assertIsNone(events[0]["action_id"])
        self.assertEqual(events[0]["sources"], ["FB US Equity"])

    def test_stable_identity_ignores_as_of_date_and_parameter_header(self):
        first, _ = parse_table(
            table(), "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        changed = table()
        changed[0][-1] = "ticker_change(effective_date=range(2021-01-01,2023-01-01))"
        changed[1][1] += 1
        second, _ = parse_table(
            changed, "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        self.assertEqual(first[0]["id"], second[0]["id"])

    def test_live_listing_and_delisting_fixtures(self):
        for kind, security, effective, announced, expected in [
            ("listings", "ARM US Equity", 45183, 45180, "2023-09-14"),
            ("delistings", "TWTR US Equity", 44862, 44860, "2022-10-28"),
        ]:
            header = [
                "ID",
                "DATES",
                "EFFECTIVE_DATE",
                "ANNOUNCED_DATE",
                f"{kind}(effective_date=range())",
            ]
            data = [header, [security, 46316, effective, announced, "UW"]]
            events, _ = parse_table(
                data,
                kind,
                options(
                    start_date=expected[:4] + "-01-01", end_date=expected[:4] + "-12-31"
                ),
                {security: [security]},
            )
            self.assertEqual(events[0]["date"], expected)

    def test_announcement_filter_does_not_use_effective_or_observation_date(self):
        events, _ = parse_table(
            table(),
            "ticker_changes",
            options(date_basis="announced"),
            {"META US Equity": ["META US Equity"]},
        )
        self.assertEqual(events, [])

    def test_missing_date_is_not_empty_success(self):
        data = table()
        data[1][2] = None
        events, warnings = parse_table(
            data, "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        self.assertFalse(events)
        self.assertIn("not evidence", warnings[0])

    def test_com_datetime_dates_are_preserved(self):
        data = table()
        data[1][2] = datetime(2022, 6, 9, tzinfo=timezone.utc)
        events, _ = parse_table(
            data, "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        self.assertEqual(events[0]["date"], "2022-06-09")

    def test_rejects_errors_pending_bad_schema_and_unexpected_security(self):
        for data, exception in [
            ([["#N/A Invalid Parameter: Undefined item"]], ValueError),
            ([["#N/A"]], ValueError),
            ([["Requesting Data..."]], PendingBql),
            ([[-2146826246]], ValueError),
            ([["ID", "DATES"]], ValueError),
            (None, PendingBql),
        ]:
            with self.assertRaises(exception):
                parse_table(data, "ticker_changes", options(), {})
        with self.assertRaisesRegex(ValueError, "outside"):
            parse_table(table(), "ticker_changes", options(), {"AAPL US Equity": []})

    def test_empty_valid_header_is_allowed(self):
        events, warnings = parse_table(table()[:1], "ticker_changes", options(), {})
        self.assertEqual((events, warnings), ([], []))

    def test_dropna_header_wrapper_is_not_mistaken_for_bad_schema(self):
        data = table()
        data[0][-1] = f"dropna({data[0][-1]}, true)"
        events, _ = parse_table(
            data, "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        self.assertEqual(events[0]["date"], "2022-06-09")

    def test_exact_live_no_data_message_is_empty_not_an_error(self):
        message = "#N/A No Data: Function dropna(ticker_change(effective_date=range(2022-01-01,2022-12-31)),true) returned empty results."
        self.assertEqual(
            parse_table(
                [[message, None], [None, None]], "ticker_changes", options(), {}
            ),
            ([], []),
        )
        with self.assertRaises(ValueError):
            parse_table(
                [[message.replace("ticker_change", "listings")]],
                "ticker_changes",
                options(),
                {},
            )

    def test_old_used_range_blank_columns_do_not_corrupt_new_query_schema(self):
        data = table()
        data[0].extend([None, None])
        data[1].extend([None, None])
        events, _ = parse_table(
            data, "ticker_changes", options(), {"META US Equity": ["META US Equity"]}
        )
        self.assertEqual(events[0]["details"]["EFFECTIVE_DATE"], "2022-06-09")

    def test_formula_uses_value_cells_and_allowlisted_items_only(self):
        formula = build_formula("ticker_changes", options(), 2)
        self.assertIn("BQL.LIST(Inputs!A2:A3)", formula)
        self.assertIn(
            "ticker_change(effective_date=range(2022-01-01,2022-12-31))", formula
        )
        self.assertNotIn("META", formula)
        self.assertIn(
            "announced_date=",
            build_formula("listings", options(date_basis="announced"), 1),
        )
        for kind, count in [("name_changes", 1), ("listings", 0), ("listings", 51)]:
            with self.assertRaises(ValueError):
                build_formula(kind, options(), count)

    def test_input_cells_reject_formula_like_symbols(self):
        validate_bql_symbols(["META US Equity", "/isin/US30303M1027"])
        for symbol in ['=WEBSERVICE("bad")', "+cmd", "@bad", "x\nAAPL", 'AAPL"bad']:
            with self.assertRaises(ValueError):
                validate_bql_symbols([symbol])


if __name__ == "__main__":
    unittest.main()
