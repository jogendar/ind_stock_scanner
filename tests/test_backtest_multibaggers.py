import csv
from datetime import date
from pathlib import Path
import tempfile
import unittest

from backtest_multibaggers import (
    LatestPrice,
    SplitEvent,
    build_observation_rows,
    build_summary_rows,
    discover_snapshots,
    read_observations,
)


class BacktestMultibaggersTests(unittest.TestCase):
    def write_snapshot(self, directory: Path, name: str, rows: list[dict[str, object]]) -> Path:
        path = directory / name
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["Symbol", "Price", "Score (%)"])
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_discovers_only_valid_dated_snapshots_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            self.write_snapshot(directory, "penny_stock_scores_02_01_26.csv", [])
            self.write_snapshot(directory, "penny_stock_scores_31_12_25.csv", [])
            self.write_snapshot(directory, "penny_stock_scores_backtest_2022-12-28.csv", [])

            snapshots = discover_snapshots(directory)

            self.assertEqual(
                [item[0] for item in snapshots],
                [date(2025, 12, 31), date(2026, 1, 2)],
            )

    def test_uses_strict_price_cutoff_and_normalizes_bare_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            path = self.write_snapshot(
                directory,
                "penny_stock_scores_01_01_26.csv",
                [
                    {"Symbol": "cheap", "Price": 19.99, "Score (%)": 60},
                    {"Symbol": "exact.NS", "Price": 20, "Score (%)": 70},
                    {"Symbol": "bad.NS", "Price": "", "Score (%)": 80},
                ],
            )

            observations, warnings = read_observations(
                [(date(2026, 1, 1), path)], 20, ".NS"
            )

            self.assertEqual(warnings, [])
            self.assertEqual(len(observations), 1)
            self.assertEqual(observations[0].symbol, "CHEAP.NS")
            self.assertEqual(observations[0].entry_price, 19.99)

    def test_first_entry_and_hindsight_low_are_kept_separate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            first_path = self.write_snapshot(
                directory,
                "penny_stock_scores_01_01_26.csv",
                [{"Symbol": "ABC.NS", "Price": 18, "Score (%)": 50}],
            )
            second_path = self.write_snapshot(
                directory,
                "penny_stock_scores_02_01_26.csv",
                [{"Symbol": "ABC.NS", "Price": 10, "Score (%)": 75}],
            )
            observations, _ = read_observations(
                [
                    (date(2026, 1, 1), first_path),
                    (date(2026, 1, 2), second_path),
                ],
                20,
                ".NS",
            )
            prices = {
                "ABC.NS": LatestPrice(
                    "ABC.NS", 25, date(2026, 9, 30), "test price"
                )
            }

            summary = build_summary_rows(observations, prices, 2)[0]
            detail = build_observation_rows(observations, prices, 2)

            self.assertEqual(summary["First Penny Price"], 18)
            self.assertEqual(summary["Lowest Penny Price"], 10)
            self.assertFalse(summary["First Entry Multibagger"])
            self.assertTrue(summary["Lowest Entry Multibagger (Hindsight)"])
            self.assertEqual([row["Is Multibagger"] for row in detail], [False, True])

    def test_reverse_split_does_not_create_false_multibagger(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            path = self.write_snapshot(
                directory,
                "penny_stock_scores_01_01_26.csv",
                [{"Symbol": "ABC.NS", "Price": 2, "Score (%)": 50}],
            )
            observations, _ = read_observations(
                [(date(2026, 1, 1), path)], 20, ".NS"
            )
            prices = {
                "ABC.NS": LatestPrice(
                    "ABC.NS", 150, date(2026, 9, 30), "test price"
                )
            }
            splits = {"ABC.NS": [SplitEvent(date(2026, 6, 1), 0.01)]}

            summary = build_summary_rows(observations, prices, 2, splits)[0]

            self.assertEqual(summary["Raw Multiple From First"], 75)
            self.assertEqual(summary["Adjusted First Entry Price"], 200)
            self.assertEqual(summary["Multiple From First"], 0.75)
            self.assertFalse(summary["First Entry Multibagger"])


if __name__ == "__main__":
    unittest.main()
