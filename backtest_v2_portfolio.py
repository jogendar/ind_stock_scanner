#!/usr/bin/env python3
"""Backtest a V2 top-N portfolio after a consecutive-session stability rule."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math
from pathlib import Path
from typing import Sequence

import pandas as pd
import yfinance as yf

from backtest_multibaggers import discover_snapshots
from score_v2 import future_multibagger_score, passes_hard_gates


SCORE_COLUMNS = {
    "Symbol",
    "Price",
    "Score (%)",
    "market_cap",
    "promoter_holding",
    "promoter_holding_growth",
    "revenue_growth_5y",
    "profit_growth_5y",
    "eps_growth_5y",
    "operating_leverage",
    "roe",
    "roce",
    "free_cash_flow",
    "cash_conversion_ratio",
    "current_ratio",
    "interest_coverage",
    "de_ratio",
    "operating_margin",
    "net_margin",
    "pb_ratio",
    "pe_ratio",
    "ev_ebitda",
    "quarterly_revenue",
    "quarterly_profit",
    "quarterly_eps",
    "audit_qualified",
    "qualified_audit",
    "auditor_qualification",
    "corporate_action_window",
    "recent_dilution",
    "dilution_pct",
}


@dataclass(frozen=True)
class SplitEvent:
    event_date: date
    factor: float


@dataclass
class MarketBundle:
    features: pd.DataFrame
    current_price: float | None
    current_date: date | None
    splits: list[SplitEvent]


def finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def latest_summary(output_dir: Path) -> Path:
    matches = sorted(output_dir.glob("multibagger_summary_*.csv"))
    if not matches:
        raise FileNotFoundError(f"no multibagger summary found under {output_dir}")
    return matches[-1]


def ticker_frame(frame: pd.DataFrame, symbol: str, symbol_count: int) -> pd.DataFrame | None:
    if frame.empty:
        return None
    if symbol_count == 1 and not isinstance(frame.columns, pd.MultiIndex):
        return frame
    if not isinstance(frame.columns, pd.MultiIndex):
        return None
    level_zero = set(frame.columns.get_level_values(0))
    level_one = set(frame.columns.get_level_values(1))
    if symbol in level_zero:
        return frame[symbol]
    if symbol in level_one:
        return frame.xs(symbol, axis=1, level=1)
    return None


def technical_feature_frame(history: pd.DataFrame) -> pd.DataFrame:
    adjusted_column = "Adj Close" if "Adj Close" in history.columns else "Close"
    close = pd.to_numeric(history[adjusted_column], errors="coerce")
    volume = pd.to_numeric(history.get("Volume"), errors="coerce").fillna(0)
    features = pd.DataFrame(index=history.index)
    features["return_1m"] = close / close.shift(21) - 1
    features["return_3m"] = close / close.shift(63) - 1
    features["return_6m"] = close / close.shift(126) - 1
    features["return_12m"] = close / close.shift(252) - 1
    features["sma_20_100"] = (
        close.rolling(20, min_periods=20).mean()
        / close.rolling(100, min_periods=100).mean()
        - 1
    )
    features["near_52w_high"] = (
        close / close.rolling(252, min_periods=50).max() - 1
    )
    features["above_52w_low"] = (
        close / close.rolling(252, min_periods=50).min() - 1
    )
    daily_returns = close.pct_change(fill_method=None)
    features["volatility_3m"] = (
        daily_returns.rolling(63, min_periods=30).std() * math.sqrt(252)
    )
    features["positive_days_3m"] = (
        daily_returns.gt(0).rolling(63, min_periods=30).mean()
    )
    features["volume_ratio_20_120"] = (
        volume.rolling(20, min_periods=20).mean()
        / volume.rolling(120, min_periods=60).mean().replace(0, pd.NA)
    )
    features["turnover_20_cr"] = (
        (close * volume).rolling(20, min_periods=20).mean() / 10_000_000
    )
    rolling_peak = close.rolling(252, min_periods=50).max()
    features["max_drawdown_1y"] = close / rolling_peak - 1
    return features.replace([math.inf, -math.inf], pd.NA)


def download_market_data(
    symbols: Sequence[str],
    snapshot_dates: Sequence[date],
    start: date,
    end: date,
    batch_size: int,
) -> dict[str, MarketBundle]:
    bundles: dict[str, MarketBundle] = {}
    target_index = pd.DatetimeIndex(snapshot_dates)
    batches = [
        symbols[offset : offset + batch_size]
        for offset in range(0, len(symbols), batch_size)
    ]
    for batch_number, batch in enumerate(batches, start=1):
        print(
            f"Fetching stock history: batch {batch_number}/{len(batches)}...",
            flush=True,
        )
        try:
            downloaded = yf.download(
                list(batch),
                start=start.isoformat(),
                end=end.isoformat(),
                group_by="ticker",
                auto_adjust=False,
                actions=True,
                threads=True,
                progress=False,
                timeout=30,
            )
        except Exception as exc:
            print(f"  Batch failed: {exc}")
            continue
        for symbol in batch:
            history = ticker_frame(downloaded, symbol, len(batch))
            if history is None or "Close" not in history.columns:
                continue
            history = history.dropna(subset=["Close"]).copy()
            if history.empty:
                continue
            if getattr(history.index, "tz", None) is not None:
                history.index = history.index.tz_localize(None)
            feature_frame = technical_feature_frame(history)
            features = feature_frame.reindex(target_index, method="ffill")
            raw_close = pd.to_numeric(history["Close"], errors="coerce").dropna()
            current_price = finite_float(raw_close.iloc[-1]) if not raw_close.empty else None
            current_date = raw_close.index[-1].date() if not raw_close.empty else None
            splits: list[SplitEvent] = []
            if "Stock Splits" in history.columns:
                for timestamp, raw_factor in history["Stock Splits"].dropna().items():
                    factor = finite_float(raw_factor)
                    if factor is not None and factor > 0:
                        splits.append(SplitEvent(timestamp.date(), factor))
            bundles[symbol] = MarketBundle(
                features=features,
                current_price=current_price,
                current_date=current_date,
                splits=splits,
            )
    return bundles


def trading_snapshot_dates(
    snapshots: Sequence[tuple[date, Path]], start: date, end: date
) -> list[date]:
    try:
        benchmark = yf.download(
            "^NSEI",
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=False,
            progress=False,
            timeout=30,
        )
        market_dates = {timestamp.date() for timestamp in benchmark.index}
        dates = [snapshot_date for snapshot_date, _ in snapshots if snapshot_date in market_dates]
        if dates:
            return dates
    except Exception as exc:
        print(f"Warning: NSE trading calendar unavailable ({exc}); using weekdays.")
    return [snapshot_date for snapshot_date, _ in snapshots if snapshot_date.weekday() < 5]


def rank_snapshot(
    path: Path,
    snapshot_date: date,
    bundles: dict[str, MarketBundle],
    price_threshold: float,
    top_n: int,
) -> list[dict[str, object]]:
    frame = pd.read_csv(
        path,
        usecols=lambda column: column in SCORE_COLUMNS,
        dtype=str,
        low_memory=False,
    )
    frame["Price"] = pd.to_numeric(frame.get("Price"), errors="coerce")
    frame = frame[(frame["Price"] > 0) & (frame["Price"] < price_threshold)]
    ranked: list[dict[str, object]] = []
    timestamp = pd.Timestamp(snapshot_date)
    for row in frame.to_dict("records"):
        symbol = str(row.get("Symbol", "")).strip().upper()
        if not symbol:
            continue
        bundle = bundles.get(symbol)
        if bundle is not None and timestamp in bundle.features.index:
            technical = bundle.features.loc[timestamp].to_dict()
            row.update(technical)
        stock_for_scoring = {"quantitative": row, "qualitative": {}}
        _, score, breakdown = future_multibagger_score(stock_for_scoring)
        if not passes_hard_gates(stock_for_scoring):
            continue
        old_score = finite_float(row.get("Score (%)")) or 0.0
        ranked.append(
            {
                "Symbol": symbol,
                "Date": snapshot_date,
                "Price": float(row["Price"]),
                "V2 Score": score,
                "Old Score": old_score,
                "Market Confirmation": breakdown["market_confirmation"],
                "Financial Quality": breakdown["financial_quality"],
                "Fundamental Growth": breakdown["fundamental_growth"],
            }
        )
    ranked.sort(
        key=lambda row: (
            -float(row["V2 Score"]),
            -float(row["Market Confirmation"]),
            -float(row["Financial Quality"]),
            -float(row["Fundamental Growth"]),
            str(row["Symbol"]),
        )
    )
    for rank, row in enumerate(ranked[:top_n], start=1):
        row["Rank"] = rank
    return ranked[:top_n]


def stable_entries(
    rankings: Sequence[tuple[date, Sequence[dict[str, object]]]],
    stable_sessions: int,
) -> list[dict[str, object]]:
    streaks: dict[str, int] = {}
    streak_starts: dict[str, date] = {}
    purchased: set[str] = set()
    entries: list[dict[str, object]] = []
    previous_top: set[str] = set()

    for snapshot_date, rows in rankings:
        by_symbol = {str(row["Symbol"]): row for row in rows}
        current_top = set(by_symbol)
        for symbol in current_top:
            if symbol in previous_top:
                streaks[symbol] = streaks.get(symbol, 0) + 1
            else:
                streaks[symbol] = 1
                streak_starts[symbol] = snapshot_date
            if streaks[symbol] == stable_sessions and symbol not in purchased:
                entry = dict(by_symbol[symbol])
                entry["Stable From"] = streak_starts[symbol]
                entry["Confirmation Sessions"] = stable_sessions
                entries.append(entry)
                purchased.add(symbol)
        for symbol in previous_top - current_top:
            streaks.pop(symbol, None)
            streak_starts.pop(symbol, None)
        previous_top = current_top
    return sorted(entries, key=lambda row: (row["Date"], row["Rank"], row["Symbol"]))


def split_factor(
    events: Sequence[SplitEvent], entry_date: date, current_date: date | None
) -> float:
    factor = 1.0
    for event in events:
        if event.event_date > entry_date and (
            current_date is None or event.event_date <= current_date
        ):
            factor *= event.factor
    return factor


def value_entries(
    entries: Sequence[dict[str, object]],
    bundles: dict[str, MarketBundle],
    investment_per_stock: float,
) -> list[dict[str, object]]:
    valued: list[dict[str, object]] = []
    for entry in entries:
        row = dict(entry)
        symbol = str(row["Symbol"])
        entry_date = row["Date"]
        entry_price = float(row["Price"])
        bundle = bundles.get(symbol)
        row["Investment"] = investment_per_stock
        row["Initial Shares"] = investment_per_stock / entry_price
        if bundle is None or bundle.current_price is None:
            row.update(
                {
                    "Split Factor": "",
                    "Current Shares": "",
                    "Current Price": "",
                    "Current Price Date": "",
                    "Price Multiple": "",
                    "Current Value": "",
                    "Profit": "",
                    "Return (%)": "",
                    "Is Multibagger": "",
                    "Valuation Status": "unresolved",
                }
            )
        else:
            factor = split_factor(bundle.splits, entry_date, bundle.current_date)
            current_shares = row["Initial Shares"] * factor
            current_value = current_shares * bundle.current_price
            multiple = current_value / investment_per_stock
            row.update(
                {
                    "Split Factor": factor,
                    "Current Shares": current_shares,
                    "Current Price": bundle.current_price,
                    "Current Price Date": (
                        bundle.current_date.isoformat() if bundle.current_date else ""
                    ),
                    "Price Multiple": multiple,
                    "Current Value": current_value,
                    "Profit": current_value - investment_per_stock,
                    "Return (%)": (multiple - 1) * 100,
                    "Is Multibagger": multiple >= 2,
                    "Valuation Status": "resolved",
                }
            )
        row["Date"] = entry_date.isoformat()
        row["Stable From"] = row["Stable From"].isoformat()
        valued.append(row)
    return valued


def write_csv(path: Path, rows: Sequence[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=Path("backtest_results"))
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--price-threshold", type=float, default=20.0)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--stable-sessions", type=int, default=20)
    parser.add_argument("--investment-per-stock", type=float, default=5_000.0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--valuation-date",
        type=date.fromisoformat,
        help=(
            "latest market date to include (YYYY-MM-DD); useful for excluding "
            "an incomplete current trading session"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.price_threshold <= 0 or args.top_n <= 0 or args.stable_sessions <= 0:
        raise SystemExit("price threshold, top N, and stable sessions must be positive")
    if args.investment_per_stock <= 0 or args.batch_size <= 0:
        raise SystemExit("investment and batch size must be positive")

    snapshots = discover_snapshots(args.input_dir)
    if not snapshots:
        print("No dated score snapshots found.")
        return 1
    snapshot_lookup = dict(snapshots)
    calendar_end = snapshots[-1][0] + timedelta(days=2)
    trading_dates = trading_snapshot_dates(
        snapshots, snapshots[0][0] - timedelta(days=7), calendar_end
    )
    print(
        f"Using {len(trading_dates)} NSE trading snapshots from "
        f"{trading_dates[0]} to {trading_dates[-1]}."
    )

    summary_path = args.summary or latest_summary(args.output_dir)
    summary = pd.read_csv(summary_path)
    symbols = sorted(summary["Symbol"].dropna().astype(str).str.upper().unique())
    print(f"Candidate universe: {len(symbols)} symbols.")
    market_start = trading_dates[0] - timedelta(days=380)
    valuation_date = args.valuation_date or datetime.now(timezone.utc).date()
    if valuation_date < trading_dates[-1]:
        raise SystemExit(
            "valuation date cannot be earlier than the final ranking snapshot"
        )
    # yfinance treats `end` as exclusive.
    market_end = valuation_date + timedelta(days=1)
    bundles = download_market_data(
        symbols, trading_dates, market_start, market_end, args.batch_size
    )
    print(f"Market histories resolved: {len(bundles)}/{len(symbols)}")

    rankings: list[tuple[date, list[dict[str, object]]]] = []
    for index, snapshot_date in enumerate(trading_dates, start=1):
        if index == 1 or index % 25 == 0 or index == len(trading_dates):
            print(f"Scoring snapshot {index}/{len(trading_dates)}: {snapshot_date}")
        rankings.append(
            (
                snapshot_date,
                rank_snapshot(
                    snapshot_lookup[snapshot_date],
                    snapshot_date,
                    bundles,
                    args.price_threshold,
                    args.top_n,
                ),
            )
        )

    entries = stable_entries(rankings, args.stable_sessions)
    valued = value_entries(entries, bundles, args.investment_per_stock)
    if not valued:
        print("No stock satisfied the stability rule.")
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    current_dates = [
        bundle.current_date for bundle in bundles.values() if bundle.current_date is not None
    ]
    report_date = max(current_dates) if current_dates else date.today()
    output_path = (
        args.output_dir
        / f"v2_top{args.top_n}_stable{args.stable_sessions}_portfolio_{report_date}.csv"
    )
    write_csv(output_path, valued)

    resolved = [row for row in valued if row["Valuation Status"] == "resolved"]
    unresolved = [row for row in valued if row["Valuation Status"] != "resolved"]
    total_invested = args.investment_per_stock * len(valued)
    known_value = sum(float(row["Current Value"]) for row in resolved)
    conservative_value = known_value
    conservative_profit = conservative_value - total_invested
    portfolio_multiple = conservative_value / total_invested
    equal_weight_5k_value = 5_000 * portfolio_multiple
    multibaggers = sum(row["Is Multibagger"] is True for row in resolved)

    print()
    print(f"Stocks purchased: {len(valued)}")
    print(f"Current values resolved: {len(resolved)}; unresolved: {len(unresolved)}")
    print(f"Capital at Rs {args.investment_per_stock:,.0f} each: Rs {total_invested:,.2f}")
    print(f"Current value (unresolved positions marked zero): Rs {conservative_value:,.2f}")
    print(f"Profit/loss: Rs {conservative_profit:,.2f}")
    print(f"Portfolio return: {(portfolio_multiple - 1) * 100:.2f}%")
    print(f"Equivalent Rs 5,000 total portfolio value: Rs {equal_weight_5k_value:,.2f}")
    print(f"Positions currently >=2x: {multibaggers}")
    if unresolved:
        print("Unresolved symbols: " + ", ".join(str(row["Symbol"]) for row in unresolved))
    print(f"Trade details saved to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
