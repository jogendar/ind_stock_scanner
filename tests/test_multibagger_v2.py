from pathlib import Path
import tempfile
import unittest

from multibagger_v2 import load_securities, load_symbols


class V2UniverseTests(unittest.TestCase):
    def test_eq_and_be_use_the_same_scanner_universe(self) -> None:
        content = (
            "SYMBOL,NAME OF COMPANY,SERIES\n"
            "NORMAL,Normal Limited,EQ\n"
            "DELIVERY,Delivery Limited, be \n"
            "SMALL,Small Limited,SM\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "equities.csv"
            path.write_text(content, encoding="utf-8")

            securities = load_securities(path)

            self.assertEqual(securities, [("NORMAL", "EQ"), ("DELIVERY", "BE")])
            self.assertEqual(load_symbols(path), ["NORMAL", "DELIVERY"])

    def test_missing_series_column_keeps_symbols_with_blank_label(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "equities.csv"
            path.write_text("SYMBOL\nABC\n", encoding="utf-8")

            self.assertEqual(load_securities(path), [("ABC", "")])


if __name__ == "__main__":
    unittest.main()
