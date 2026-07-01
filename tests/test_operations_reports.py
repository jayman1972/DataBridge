import unittest

from sggg.operations_reports import (
    DEFAULT_ETF_SECURITIES,
    format_etf_securities_text,
    parse_etf_securities_text,
)


class OperationsReportsParseTest(unittest.TestCase):
    def test_parse_etf_list(self):
        parsed = parse_etf_securities_text("SPY.US, ehf100i\nQQQ.US")
        self.assertEqual(parsed, ["SPY.US", "EHF100I", "QQQ.US"])

    def test_reject_invalid_token(self):
        with self.assertRaises(ValueError):
            parse_etf_securities_text("SPY@US")

    def test_defaults_round_trip(self):
        text = format_etf_securities_text(DEFAULT_ETF_SECURITIES)
        self.assertGreater(len(parse_etf_securities_text(text)), 50)


if __name__ == "__main__":
    unittest.main()
