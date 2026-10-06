"""Point-in-time market features used by the v2 multibagger score."""

from __future__ import annotations

import math
from typing import Any


TRADING_DAYS = {
    "return_1m": 21,
    "return_3m": 63,
    "return_6m": 126,
    "return_12m": 252,
}


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def calculate_market_features(history: Any) -> dict[str, float | None]:
    """Calculate features using only rows present in ``history``.

    During a historical backtest the caller must trim the frame at the signal
    date before calling this function. That keeps every feature point-in-time
    and prevents future-price leakage.
    """
    features: dict[str, float | None] = {
        **{name: None for name in TRADING_DAYS},
        "sma_20_100": None,
        "near_52w_high": None,
        "above_52w_low": None,
        "volatility_3m": None,
        "positive_days_3m": None,
        "volume_ratio_20_120": None,
        "turnover_20_cr": None,
        "max_drawdown_1y": None,
    }
    if history is None or getattr(history, "empty", True):
        return features
    if "Close" not in history.columns:
        return features

    close = history["Close"].dropna().astype(float)
    if close.empty:
        return features
    volume = (
        history["Volume"].reindex(close.index).fillna(0).astype(float)
        if "Volume" in history.columns
        else None
    )

    for name, lookback in TRADING_DAYS.items():
        if len(close) > lookback:
            previous = _finite(close.iloc[-lookback - 1])
            current = _finite(close.iloc[-1])
            if previous is not None and previous > 0 and current is not None:
                features[name] = current / previous - 1

    if len(close) >= 100:
        long_average = _finite(close.tail(100).mean())
        short_average = _finite(close.tail(20).mean())
        if long_average is not None and long_average > 0 and short_average is not None:
            features["sma_20_100"] = short_average / long_average - 1

    if len(close) >= 50:
        yearly_close = close.tail(252)
        current = _finite(yearly_close.iloc[-1])
        high = _finite(yearly_close.max())
        low = _finite(yearly_close.min())
        if current is not None and high is not None and high > 0:
            features["near_52w_high"] = current / high - 1
        if current is not None and low is not None and low > 0:
            features["above_52w_low"] = current / low - 1
        running_peak = yearly_close.cummax()
        features["max_drawdown_1y"] = _finite((yearly_close / running_peak - 1).min())

    daily_returns = close.pct_change(fill_method=None).dropna()
    if len(daily_returns) >= 30:
        recent_returns = daily_returns.tail(63)
        volatility = _finite(recent_returns.std())
        if volatility is not None:
            features["volatility_3m"] = volatility * math.sqrt(252)
        features["positive_days_3m"] = _finite((recent_returns > 0).mean())

    if volume is not None:
        if len(volume) >= 60:
            baseline_volume = _finite(volume.tail(120).mean())
            recent_volume = _finite(volume.tail(20).mean())
            if (
                baseline_volume is not None
                and baseline_volume > 0
                and recent_volume is not None
            ):
                features["volume_ratio_20_120"] = recent_volume / baseline_volume
        if len(volume) >= 20:
            turnover = _finite((close.tail(20) * volume.tail(20)).mean())
            if turnover is not None:
                features["turnover_20_cr"] = turnover / 10_000_000

    return features
