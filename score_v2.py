"""Explainable, backtest-informed scoring for future multibagger potential.

The score intentionally ranks potential rather than estimating a probability.
It has six capped pillars totaling 100 points, followed by transparent risk
penalties. Missing values earn no points instead of being silently treated as
good or bad, except that extreme valuation requires enough data to run the two
core cash-generation and capital-return risk checks.
"""

from __future__ import annotations

import math
from typing import Any


CANDIDATE_SCORE = 50.0
LEGACY_RISK_CAP = 15
MAX_RISK_PENALTY = 30
FRAGILE_SCORE_CAP = CANDIDATE_SCORE - 1


def _value(metrics: dict[str, Any], key: str) -> float | None:
    try:
        value = float(metrics.get(key))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if not isinstance(value, str):
        return False
    normalized = value.strip().lower()
    if normalized in {"", "0", "false", "no", "none", "na", "n/a", "unmodified"}:
        return False
    return normalized in {"1", "true", "yes", "y", "qualified", "modified"}


def _growth_points(value: float | None, maximum: int) -> int:
    if value is None or value <= 0:
        return 0
    if value >= 0.20:
        return maximum
    if value >= 0.10:
        return round(maximum * 0.70)
    return round(maximum * 0.35)


def hard_gate_reasons(stock: dict[str, Any]) -> tuple[str, ...]:
    """Return deterministic reasons that disqualify a ranked candidate."""
    metrics = stock.get("quantitative", stock)
    free_cash_flow = _value(metrics, "free_cash_flow")
    roe = _value(metrics, "roe")
    roce = _value(metrics, "roce")
    capital_returns = [value for value in (roe, roce) if value is not None]
    capital_efficiency = max(capital_returns) if capital_returns else None
    price_to_earnings = _value(metrics, "pe_ratio")
    ev_to_ebitda = _value(metrics, "ev_ebitda")
    extreme_valuation = (
        price_to_earnings is not None and price_to_earnings > 50
    ) or (ev_to_ebitda is not None and ev_to_ebitda > 30)

    reasons: list[str] = []
    if (
        free_cash_flow is not None
        and free_cash_flow < 0
        and capital_efficiency is not None
        and capital_efficiency < 0.05
        and extreme_valuation
    ):
        reasons.append("compound_fragility")
    if free_cash_flow is None and capital_efficiency is None and extreme_valuation:
        reasons.append("extreme_valuation_missing_core_risk_data")
    return tuple(reasons)


def passes_hard_gates(stock: dict[str, Any]) -> bool:
    """Return whether a stock is eligible for ranked candidate lists."""
    return not hard_gate_reasons(stock)


def future_multibagger_score(
    stock: dict[str, Any],
) -> tuple[float, float, dict[str, float]]:
    """Return ``(points, percentage, pillar breakdown)`` for one stock."""
    metrics = stock.get("quantitative", stock)
    qualitative = stock.get("qualitative", {})

    # 1. Size runway (15): small companies have more room to multiply, but
    # larger low-priced companies remain eligible rather than being filtered.
    market_cap = _value(metrics, "market_cap")
    market_cap_cr = market_cap / 10_000_000 if market_cap is not None else None
    runway = (
        15
        if market_cap_cr is not None and market_cap_cr <= 25
        else 13
        if market_cap_cr is not None and market_cap_cr <= 50
        else 10
        if market_cap_cr is not None and market_cap_cr <= 100
        else 7
        if market_cap_cr is not None and market_cap_cr <= 500
        else 4
        if market_cap_cr is not None and market_cap_cr <= 2_000
        else 2
        if market_cap_cr is not None and market_cap_cr <= 5_000
        else 0
    )

    # 2. Ownership alignment (10).
    promoter = _value(metrics, "promoter_holding")
    ownership = (
        10
        if promoter is not None and promoter >= 65
        else 8
        if promoter is not None and promoter >= 50
        else 5
        if promoter is not None and promoter >= 35
        else 2
        if promoter is not None and promoter >= 20
        else 0
    )
    promoter_growth = _value(metrics, "promoter_holding_growth")
    if promoter_growth is not None and promoter_growth > 0.5:
        ownership = min(10, ownership + 2)

    # 3. Fundamental growth (20). Keep the original V2 weights because the
    # archive showed useful ranking lift from them. Risk controls below deal
    # with the weak-quality growth cases instead of removing the signal.
    growth = 0
    growth += _growth_points(_value(metrics, "revenue_growth_5y"), 7)
    growth += _growth_points(_value(metrics, "profit_growth_5y"), 7)
    growth += _growth_points(_value(metrics, "eps_growth_5y"), 4)
    operating_leverage = _value(metrics, "operating_leverage")
    growth += (
        2
        if operating_leverage is not None and operating_leverage >= 1.2
        else 1
        if operating_leverage is not None and operating_leverage >= 0.8
        else 0
    )

    # 4. Financial survival and self-funding (20). Yahoo reports debt/equity
    # as a percentage, so 50 means 0.5x—not 50x. Capital efficiency is used as
    # a risk check rather than a positive-weight replacement because ROE/ROCE
    # coverage is sparse in the archived snapshots.
    quality = 0
    free_cash_flow = _value(metrics, "free_cash_flow")
    quality += 5 if free_cash_flow is not None and free_cash_flow > 0 else 0
    current_ratio = _value(metrics, "current_ratio")
    quality += (
        5
        if current_ratio is not None and current_ratio >= 1.5
        else 2
        if current_ratio is not None and current_ratio >= 1.0
        else 0
    )
    interest_coverage = _value(metrics, "interest_coverage")
    quality += (
        4
        if interest_coverage is not None and interest_coverage >= 3
        else 2
        if interest_coverage is not None and interest_coverage >= 2
        else 1
        if interest_coverage is not None and interest_coverage >= 1
        else 0
    )
    debt_to_equity = _value(metrics, "de_ratio")
    quality += (
        3
        if debt_to_equity is not None and debt_to_equity < 50
        else 2
        if debt_to_equity is not None and debt_to_equity < 100
        else 1
        if debt_to_equity is not None and debt_to_equity < 200
        else 0
    )
    operating_margin = _value(metrics, "operating_margin")
    quality += (
        2
        if operating_margin is not None and operating_margin > 0.10
        else 1
        if operating_margin is not None and operating_margin > 0
        else 0
    )
    net_margin = _value(metrics, "net_margin")
    quality += 1 if net_margin is not None and net_margin > 0 else 0

    roe = _value(metrics, "roe")
    roce = _value(metrics, "roce")
    capital_returns = [value for value in (roe, roce) if value is not None]
    capital_efficiency = max(capital_returns) if capital_returns else None

    # 5. Valuation sanity (10). Negative multiples represent losses and earn
    # zero; the old score accidentally gave them maximum value points.
    valuation = 0
    price_to_book = _value(metrics, "pb_ratio")
    valuation += (
        5
        if price_to_book is not None and 0 < price_to_book <= 1
        else 3
        if price_to_book is not None and 0 < price_to_book <= 2.5
        else 1
        if price_to_book is not None and 0 < price_to_book <= 4
        else 0
    )
    price_to_earnings = _value(metrics, "pe_ratio")
    valuation += (
        3
        if price_to_earnings is not None and 0 < price_to_earnings <= 15
        else 2
        if price_to_earnings is not None and 0 < price_to_earnings <= 25
        else 0
    )
    ev_to_ebitda = _value(metrics, "ev_ebitda")
    valuation += (
        2
        if ev_to_ebitda is not None and 0 < ev_to_ebitda <= 10
        else 1
        if ev_to_ebitda is not None and 0 < ev_to_ebitda <= 15
        else 0
    )

    # 6. Accumulation and trend confirmation (25). Keep rebound volume as a
    # signal: historical winners often first appeared while price was weak.
    # A known corporate-action window is the exception because mechanically
    # elevated volume is not evidence of accumulation.
    market_confirmation = 0
    volume_ratio = _value(metrics, "volume_ratio_20_120")
    volume_points = (
        10
        if volume_ratio is not None and volume_ratio >= 2
        else 8
        if volume_ratio is not None and volume_ratio >= 1.25
        else 5
        if volume_ratio is not None and volume_ratio >= 1
        else 2
        if volume_ratio is not None and volume_ratio >= 0.8
        else 0
    )
    moving_average_trend = _value(metrics, "sma_20_100")
    return_3m = _value(metrics, "return_3m")
    corporate_action_window = _truthy(
        metrics.get("corporate_action_window", qualitative.get("corporate_action_window"))
    )
    if corporate_action_window:
        volume_points = 0
    market_confirmation += volume_points

    market_confirmation += (
        5
        if moving_average_trend is not None and moving_average_trend >= 0.10
        else 4
        if moving_average_trend is not None and moving_average_trend >= 0
        else 3
        if moving_average_trend is not None and moving_average_trend >= -0.05
        else 1
        if moving_average_trend is not None and moving_average_trend >= -0.15
        else 0
    )
    market_confirmation += (
        4
        if return_3m is not None and 0.10 <= return_3m <= 0.80
        else 3
        if return_3m is not None and 0 <= return_3m < 0.10
        else 2
        if return_3m is not None and -0.20 <= return_3m < 0
        else 1
        if return_3m is not None and -0.40 <= return_3m < -0.20
        else 0
    )
    distance_from_high = _value(metrics, "near_52w_high")
    market_confirmation += (
        3
        if distance_from_high is not None and distance_from_high >= -0.15
        else 2
        if distance_from_high is not None and distance_from_high >= -0.30
        else 1
        if distance_from_high is not None and distance_from_high >= -0.50
        else 0
    )
    turnover_cr = _value(metrics, "turnover_20_cr")
    market_confirmation += (
        3
        if turnover_cr is not None and turnover_cr >= 1
        else 2
        if turnover_cr is not None and turnover_cr >= 0.1
        else 1
        if turnover_cr is not None and turnover_cr >= 0.01
        else 0
    )

    # Preserve the original V2 downside penalties and their 15-point cap.
    # Add only small, independent overlays for the weaknesses exposed by the
    # failure review; large single-factor deductions hurt ranking quality in
    # the broad archive.
    legacy_penalty = 0
    if promoter is not None and promoter < 10:
        legacy_penalty += 4
    if turnover_cr is not None and turnover_cr < 0.001:
        legacy_penalty += 4
    if current_ratio is not None and current_ratio < 0.5:
        legacy_penalty += 3
    if interest_coverage is not None and interest_coverage < 0:
        legacy_penalty += 2
    if debt_to_equity is not None and debt_to_equity > 300:
        legacy_penalty += 3
    if net_margin is not None and net_margin < -0.50:
        legacy_penalty += 3
    return_1m = _value(metrics, "return_1m")
    if return_1m is not None and return_1m > 1:
        legacy_penalty += 5
    legacy_penalty = min(legacy_penalty, LEGACY_RISK_CAP)

    overlay_penalty = 0
    if free_cash_flow is not None and free_cash_flow < 0:
        overlay_penalty += 2

    if capital_efficiency is not None and capital_efficiency < 0.05:
        overlay_penalty += 2

    if price_to_earnings is not None and price_to_earnings > 50:
        overlay_penalty += 2
    if ev_to_ebitda is not None and ev_to_ebitda > 30:
        overlay_penalty += 2

    return_6m = _value(metrics, "return_6m")
    return_12m = _value(metrics, "return_12m")
    if (
        return_6m is not None
        and return_12m is not None
        and return_6m < -0.25
        and return_12m < -0.25
    ):
        overlay_penalty += 2

    event_penalty = 0
    audit_qualified = any(
        _truthy(metrics.get(key, qualitative.get(key)))
        for key in ("audit_qualified", "qualified_audit", "auditor_qualification")
    )
    if audit_qualified:
        event_penalty += 10

    dilution_pct = _value(metrics, "dilution_pct")
    if dilution_pct is None:
        dilution_pct = _value(qualitative, "dilution_pct")
    if dilution_pct is not None:
        if 0 < dilution_pct <= 1:
            dilution_pct *= 100
        event_penalty += 4 if dilution_pct >= 25 else 2 if dilution_pct >= 10 else 0
    elif _truthy(metrics.get("recent_dilution", qualitative.get("recent_dilution"))):
        event_penalty += 2

    penalty = min(
        legacy_penalty + overlay_penalty + event_penalty,
        MAX_RISK_PENALTY,
    )

    breakdown = {
        "size_runway": float(runway),
        "ownership_alignment": float(ownership),
        "fundamental_growth": float(growth),
        "financial_quality": float(quality),
        "valuation_sanity": float(valuation),
        "market_confirmation": float(market_confirmation),
        "risk_penalty": float(-penalty),
        "compound_fragility_gate": 0.0,
        "extreme_valuation_data_gate": 0.0,
    }
    points = max(0.0, min(100.0, sum(breakdown.values())))
    gate_reasons = hard_gate_reasons(stock)
    if "compound_fragility" in gate_reasons and points > FRAGILE_SCORE_CAP:
        breakdown["compound_fragility_gate"] = FRAGILE_SCORE_CAP - points
        points = FRAGILE_SCORE_CAP
    if (
        "extreme_valuation_missing_core_risk_data" in gate_reasons
        and points > FRAGILE_SCORE_CAP
    ):
        breakdown["extreme_valuation_data_gate"] = FRAGILE_SCORE_CAP - points
        points = FRAGILE_SCORE_CAP
    return points, points, breakdown


def score_band(score: float) -> str:
    if score >= 65:
        return "high-potential"
    if score >= CANDIDATE_SCORE:
        return "watchlist"
    if score >= 35:
        return "speculative"
    return "avoid"
