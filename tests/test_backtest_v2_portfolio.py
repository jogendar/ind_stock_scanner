from datetime import date
from pathlib import Path
import tempfile
import unittest

from backtest_v2_portfolio import rank_snapshot, stable_entries


class StableEntriesTests(unittest.TestCase):
    def test_rankings_exclude_hard_gated_candidates(self) -> None:
        header = (
            "Symbol,Price,market_cap,promoter_holding,revenue_growth_5y,"
            "profit_growth_5y,eps_growth_5y,operating_leverage,free_cash_flow,"
            "roe,roce,pe_ratio,ev_ebitda\n"
        )
        fragile = (
            "FRAGILE.NS,10,5000000000,55,0.3,0.3,0.3,2,-100,"
            "0.02,0.03,60,35\n"
        )
        ordinary = (
            "ORDINARY.NS,10,5000000000,55,0.3,0.3,0.3,2,100,"
            "0.12,0.10,20,12\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.csv"
            path.write_text(header + fragile + ordinary, encoding="utf-8")
            ranked = rank_snapshot(path, date(2026, 1, 2), {}, 20, 10)

        self.assertEqual([row["Symbol"] for row in ranked], ["ORDINARY.NS"])

    def test_buys_on_confirmation_session_without_lookahead(self) -> None:
        rankings = []
        for day in range(1, 5):
            rankings.append(
                (
                    date(2026, 1, day),
                    [
                        {
                            "Symbol": "ABC.NS",
                            "Date": date(2026, 1, day),
                            "Price": day,
                            "Rank": 1,
                        }
                    ],
                )
            )

        entries = stable_entries(rankings, stable_sessions=3)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["Stable From"], date(2026, 1, 1))
        self.assertEqual(entries[0]["Date"], date(2026, 1, 3))
        self.assertEqual(entries[0]["Price"], 3)

    def test_leaving_top_list_resets_streak(self) -> None:
        rankings = [
            (date(2026, 1, 1), [{"Symbol": "ABC.NS", "Date": date(2026, 1, 1), "Rank": 1}]),
            (date(2026, 1, 2), []),
            (date(2026, 1, 3), [{"Symbol": "ABC.NS", "Date": date(2026, 1, 3), "Rank": 1}]),
            (date(2026, 1, 4), [{"Symbol": "ABC.NS", "Date": date(2026, 1, 4), "Rank": 1}]),
        ]

        entries = stable_entries(rankings, stable_sessions=2)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["Stable From"], date(2026, 1, 3))
        self.assertEqual(entries[0]["Date"], date(2026, 1, 4))


if __name__ == "__main__":
    unittest.main()
