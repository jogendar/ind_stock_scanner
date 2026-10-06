import unittest

import pandas as pd

from market_features import calculate_market_features
from score_v2 import future_multibagger_score, passes_hard_gates, score_band


class ScoreV2Tests(unittest.TestCase):
    def test_missing_data_is_safe_and_earns_no_points(self) -> None:
        points, percentage, breakdown = future_multibagger_score({"quantitative": {}})

        self.assertEqual(points, 0)
        self.assertEqual(percentage, 0)
        self.assertEqual(sum(breakdown.values()), 0)

    def test_negative_valuation_multiples_are_not_rewarded(self) -> None:
        _, _, breakdown = future_multibagger_score(
            {
                "quantitative": {
                    "pb_ratio": -1,
                    "pe_ratio": -4,
                    "ev_ebitda": -2,
                }
            }
        )

        self.assertEqual(breakdown["valuation_sanity"], 0)

    def test_debt_to_equity_uses_yahoo_percentage_scale(self) -> None:
        _, _, low_debt = future_multibagger_score(
            {"quantitative": {"de_ratio": 40}}
        )
        _, _, medium_debt = future_multibagger_score(
            {"quantitative": {"de_ratio": 150}}
        )

        self.assertEqual(low_debt["financial_quality"], 3)
        self.assertEqual(medium_debt["financial_quality"], 1)

    def test_volume_accumulation_increases_market_confirmation(self) -> None:
        base = {
            "sma_20_100": 0,
            "return_3m": 0,
            "near_52w_high": -0.2,
            "turnover_20_cr": 1,
        }
        _, _, quiet = future_multibagger_score(
            {"quantitative": {**base, "volume_ratio_20_120": 0.5}}
        )
        _, _, accumulating = future_multibagger_score(
            {"quantitative": {**base, "volume_ratio_20_120": 2.0}}
        )

        self.assertEqual(
            accumulating["market_confirmation"] - quiet["market_confirmation"],
            10,
        )

    def test_known_corporate_action_volume_is_not_called_accumulation(self) -> None:
        base = {
            "return_3m": -0.15,
            "sma_20_100": -0.05,
            "near_52w_high": -0.30,
            "positive_days_3m": 0.35,
            "turnover_20_cr": 1,
        }
        _, _, ordinary = future_multibagger_score(
            {"quantitative": {**base, "volume_ratio_20_120": 3.0}}
        )
        _, _, corporate_action = future_multibagger_score(
            {
                "quantitative": {
                    **base,
                    "volume_ratio_20_120": 3.0,
                    "corporate_action_window": True,
                }
            }
        )

        self.assertEqual(
            ordinary["market_confirmation"]
            - corporate_action["market_confirmation"],
            10,
        )

    def test_weak_cash_returns_and_extreme_valuation_trigger_penalties(self) -> None:
        stock = {
            "quantitative": {
                "market_cap": 8_000_000_000,
                "promoter_holding": 44,
                "revenue_growth_5y": 0.30,
                "profit_growth_5y": 0.45,
                "eps_growth_5y": 0.25,
                "operating_leverage": 1.5,
                "profit": 100_000_000,
                "free_cash_flow": -10_000_000,
                "current_ratio": 1.5,
                "interest_coverage": 5,
                "de_ratio": 25,
                "operating_margin": 0.12,
                "net_margin": 0.04,
                "roe": 0.035,
                "roce": 0.025,
                "pb_ratio": 2,
                "pe_ratio": 61,
                "ev_ebitda": 42,
                "quarterly_revenue": [110, 108, 105, 103, 100],
                "quarterly_profit": [11, 10, 10, 9, 9],
                "volume_ratio_20_120": 2.5,
                "sma_20_100": 0.15,
                "return_3m": 0.25,
                "near_52w_high": -0.20,
                "positive_days_3m": 0.52,
                "turnover_20_cr": 2,
            }
        }
        points, _, breakdown = future_multibagger_score(stock)

        self.assertEqual(points, 49)
        self.assertEqual(breakdown["risk_penalty"], -8)
        self.assertLess(breakdown["compound_fragility_gate"], 0)
        self.assertFalse(passes_hard_gates(stock))

    def test_extreme_valuation_requires_core_risk_data(self) -> None:
        stock = {
            "quantitative": {
                "market_cap": 5_000_000_000,
                "promoter_holding": 55,
                "revenue_growth_5y": 0.30,
                "profit_growth_5y": 0.30,
                "eps_growth_5y": 0.30,
                "operating_leverage": 2,
                "pe_ratio": 60,
                "ev_ebitda": 35,
                "volume_ratio_20_120": 2,
                "sma_20_100": 0.10,
                "return_3m": 0.20,
                "near_52w_high": -0.10,
                "turnover_20_cr": 1,
            }
        }
        points, _, breakdown = future_multibagger_score(stock)

        self.assertEqual(points, 49)
        self.assertLess(breakdown["extreme_valuation_data_gate"], 0)
        self.assertFalse(passes_hard_gates(stock))

    def test_audit_and_dilution_flags_are_optional_explicit_penalties(self) -> None:
        metrics = {
            "market_cap": 200_000_000,
            "promoter_holding": 60,
            "revenue_growth_5y": 0.15,
            "profit_growth_5y": 0.15,
        }
        clean_score, _, _ = future_multibagger_score({"quantitative": metrics})
        flagged_score, _, breakdown = future_multibagger_score(
            {
                "quantitative": metrics,
                "qualitative": {
                    "audit_qualified": True,
                    "dilution_pct": 25,
                },
            }
        )

        self.assertEqual(clean_score - flagged_score, 14)
        self.assertEqual(breakdown["risk_penalty"], -14)

    def test_ideal_metrics_are_capped_at_100(self) -> None:
        points, percentage, breakdown = future_multibagger_score(
            {
                "quantitative": {
                    "market_cap": 100_000_000,
                    "promoter_holding": 70,
                    "promoter_holding_growth": 1,
                    "revenue_growth_5y": 0.30,
                    "profit_growth_5y": 0.30,
                    "eps_growth_5y": 0.30,
                    "operating_leverage": 2,
                    "free_cash_flow": 1,
                    "cash_conversion_ratio": 1,
                    "current_ratio": 2,
                    "interest_coverage": 5,
                    "de_ratio": 20,
                    "operating_margin": 0.20,
                    "net_margin": 0.10,
                    "roe": 0.20,
                    "roce": 0.18,
                    "pb_ratio": 0.8,
                    "pe_ratio": 10,
                    "ev_ebitda": 8,
                    "quarterly_revenue": [150, 130, 120, 110, 100],
                    "quarterly_profit": [30, 25, 20, 15, 10],
                    "volume_ratio_20_120": 2,
                    "sma_20_100": 0.10,
                    "return_3m": 0.30,
                    "near_52w_high": -0.10,
                    "turnover_20_cr": 2,
                }
            }
        )

        self.assertEqual(points, 100)
        self.assertEqual(percentage, 100)
        self.assertEqual(sum(breakdown.values()), 100)

    def test_market_features_use_trailing_rows(self) -> None:
        index = pd.date_range("2025-01-01", periods=130, freq="B")
        history = pd.DataFrame(
            {
                "Close": [100 + index for index in range(130)],
                "Volume": [100] * 110 + [300] * 20,
            },
            index=index,
        )

        features = calculate_market_features(history)

        self.assertGreater(features["return_3m"], 0)
        self.assertGreater(features["sma_20_100"], 0)
        self.assertGreater(features["volume_ratio_20_120"], 1)

    def test_score_bands(self) -> None:
        self.assertEqual(score_band(65), "high-potential")
        self.assertEqual(score_band(50), "watchlist")
        self.assertEqual(score_band(35), "speculative")
        self.assertEqual(score_band(34.9), "avoid")


if __name__ == "__main__":
    unittest.main()
