import unittest

from sggg.operations_reports import format_etf_securities_text, parse_etf_securities_text

_SAMPLE_ETFS = ("SPY.US", "EHF100I", "QQQ.US", "U.U.CA")


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


if __name__ == "__main__":
    unittest.main()
