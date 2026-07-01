import unittest

from sggg.operations_reports import (
    REPORT_COUNTRY_BREAKDOWN,
    REPORT_ETFS,
    _append_csv,
    _compact_add_days,
    _max_posn_date_in_csv,
    format_etf_securities_text,
    parse_etf_securities_text,
    resolve_incremental_query_range,
)

_SAMPLE_ETFS = ("SPY.US", "EHF100I", "QQQ.US", "U.U.CA")

_COUNTRY_HEADER = (
    "POSN_DATE,PORTFOLIO,SECURITY_TYPE,COUNTRY,LONG_SHORT,VALUE\r\n"
)
_COUNTRY_ROW_A = "20260301,EHP Adv Alt,Equity,US,Long,1000\r\n"
_COUNTRY_ROW_B = "20260331,EHP Adv Alt,Equity,US,Long,1100\r\n"


class OperationsReportsParseTest(unittest.TestCase):
    def test_parse_etf_list(self):
        parsed = parse_etf_securities_text("SPY.US, ehf100i\nQQQ.US")
        self.assertEqual(parsed, ["SPY.US", "EHF100I", "QQQ.US"])

    def test_reject_invalid_token(self):
        with self.assertRaises(ValueError):
            parse_etf_securities_text("SPY@US")

    def test_format_sorts_alphabetically(self):
        text = format_etf_securities_text(list(reversed(_SAMPLE_ETFS)))
        self.assertEqual(text.split(", "), sorted(_SAMPLE_ETFS))


class OperationsReportsIncrementalTest(unittest.TestCase):
    def test_max_posn_date(self):
        csv_text = _COUNTRY_HEADER + _COUNTRY_ROW_A + _COUNTRY_ROW_B
        self.assertEqual(_max_posn_date_in_csv(csv_text), "20260331")

    def test_append_csv_skips_duplicate_header(self):
        existing = _COUNTRY_HEADER + _COUNTRY_ROW_A
        new_chunk = _COUNTRY_HEADER + _COUNTRY_ROW_B
        merged = _append_csv(existing, new_chunk)
        self.assertEqual(merged.count("POSN_DATE,PORTFOLIO"), 1)
        self.assertIn(_COUNTRY_ROW_B.strip(), merged)

    def test_compact_add_days(self):
        self.assertEqual(_compact_add_days("20260331", 1), "20260401")

    def test_resolve_no_existing_file(self):
        plan = resolve_incremental_query_range(
            report_type=REPORT_COUNTRY_BREAKDOWN,
            requested_start_compact="20180818",
            end_compact="20260630",
            incremental=True,
            force_full_rebuild=False,
        )
        self.assertEqual(plan["query_start_compact"], "20180818")
        self.assertFalse(plan["incremental"])

    def test_resolve_etf_list_change_forces_full(self):
        plan = resolve_incremental_query_range(
            report_type=REPORT_ETFS,
            requested_start_compact="20180818",
            end_compact="20260630",
            incremental=True,
            force_full_rebuild=False,
            etf_securities=["SPY.US"],
        )
        self.assertEqual(plan["reason"], "no_existing_file")


if __name__ == "__main__":
    unittest.main()
