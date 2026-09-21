import hashlib
import tempfile
import unittest
from pathlib import Path

from sggg.trade_file_export import save_alphadesk_trade_file


def _csv(cusip: str = "594918104", quantity: int = 100) -> str:
    return (
        "\ufeffTrade Date,Symbol,Order Quantity,CUSIP\r\n"
        f'2026-09-21,MSFT,{quantity},"{cusip}"'
    )


class TradeFileExportTest(unittest.TestCase):
    def test_writes_and_atomically_overwrites_the_fixed_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            first = _csv(quantity=100)
            second = _csv(quantity=200)
            kwargs = {
                "filename": "Pending_Trade_Import_20260921.csv",
                "target_dir": Path(directory),
            }
            first_result = save_alphadesk_trade_file(
                **kwargs,
                csv_content=first,
                expected_sha256=hashlib.sha256(first.encode("utf-8")).hexdigest(),
            )
            second_result = save_alphadesk_trade_file(
                **kwargs,
                csv_content=second,
                expected_sha256=hashlib.sha256(second.encode("utf-8")).hexdigest(),
            )

            destination = Path(directory) / kwargs["filename"]
            self.assertFalse(first_result["overwritten"])
            self.assertTrue(second_result["overwritten"])
            self.assertEqual(destination.read_bytes(), second.encode("utf-8"))
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_rejects_path_traversal(self):
        content = _csv()
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "filename is invalid"),
        ):
            save_alphadesk_trade_file(
                filename="../Pending_Trade_Import_20260921.csv",
                csv_content=content,
                expected_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                target_dir=directory,
            )

    def test_rejects_scientific_notation_cusip(self):
        content = _csv("5.94918E+08")
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "scientific notation"),
        ):
            save_alphadesk_trade_file(
                filename="Pending_Trade_Import_20260921.csv",
                csv_content=content,
                expected_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                target_dir=directory,
            )

    def test_rejects_sha_mismatch(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "does not match"),
        ):
            save_alphadesk_trade_file(
                filename="Pending_Trade_Import_20260921.csv",
                csv_content=_csv(),
                expected_sha256="0" * 64,
                target_dir=directory,
            )


if __name__ == "__main__":
    unittest.main()
