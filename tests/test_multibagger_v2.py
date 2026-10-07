from pathlib import Path
import tempfile
import unittest

from multibagger_v2 import (
    error_rate_exceeded,
    load_securities,
    load_snapshot_candidates,
    load_symbols,
    score_snapshot_stock,
)


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

    def test_v1_snapshot_is_prefiltered_without_refetching_fundamentals(self) -> None:
        content = (
            "Symbol,Price,Market Cap (Cr),market_cap,Score (%)\n"
            "NORMAL.NS,24.9,100,1000000000,60\n"
            "DELIVERY.NS,22,200,2000000000,70\n"
            "EXPENSIVE.NS,25,50,500000000,80\n"
            "SMALL.NS,10,10,100000000,90\n"
        )
        series = {
            "NORMAL.NS": "EQ",
            "DELIVERY.NS": "BE",
            "EXPENSIVE.NS": "EQ",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "v1.csv"
            path.write_text(content, encoding="utf-8")

            records = load_snapshot_candidates(path, series, 25, None)

        self.assertEqual(
            [(record["Symbol"], record["NSE Series"]) for record in records],
            [("NORMAL.NS", "EQ"), ("DELIVERY.NS", "BE")],
        )

    def test_be_label_does_not_change_snapshot_score(self) -> None:
        record = {
            "Symbol": "DELIVERY.NS",
            "Price": 22.0,
            "Market Cap (Cr)": 200.0,
            "market_cap": 2_000_000_000.0,
            "stock_price": 22.0,
            "Score (%)": 70.0,
            "promoter_holding": 60.0,
            "revenue_growth_5y": 0.2,
            "profit_growth_5y": 0.2,
            "free_cash_flow": 1.0,
        }
        eq_result = score_snapshot_stock(
            {**record, "NSE Series": "EQ"}, None, candidate_score=50
        )
        be_result = score_snapshot_stock(
            {**record, "NSE Series": "BE"}, None, candidate_score=50
        )

        self.assertEqual(eq_result["V2 Potential Score"], be_result["V2 Potential Score"])
        self.assertEqual(eq_result["Candidate"], be_result["Candidate"])
        self.assertEqual(be_result["NSE Series"], "BE")

    def test_excessive_provider_errors_are_rejected(self) -> None:
        self.assertFalse(error_rate_exceeded(1, 10, 0.10))
        self.assertTrue(error_rate_exceeded(2, 10, 0.10))


if __name__ == "__main__":
    unittest.main()
