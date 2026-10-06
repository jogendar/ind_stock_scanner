#!/usr/bin/env python3
"""Compare the old and v2 scores against split-adjusted current outcomes."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import math
from pathlib import Path
from typing import Sequence

import pandas as pd
import yfinance as yf

from market_features import calculate_market_features
from score_v2 import (
    CANDIDATE_SCORE,
    future_multibagger_score,
    hard_gate_reasons,
    score_band,
)


def latest_summary(output_dir: Path) -> Path:
    matches = sorted(output_dir.glob("multibagger_summary_*.csv"))
    if not matches:
        raise FileNotFoundError(
            f"no multibagger_summary_YYYY-MM-DD.csv found under {output_dir}"
        )
    return matches[-1]


def report_date_from_path(path: Path) -> date:
    try:
        return date.fromisoformat(path.stem.removeprefix("multibagger_summary_"))
    except ValueError:
        return date.today()


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


def fetch_history(
    symbols: Sequence[str], start: date, end: date, batch_size: int
) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    batches = [
        symbols[offset : offset + batch_size]
        for offset in range(0, len(symbols), batch_size)
    ]
    for index, batch in enumerate(batches, start=1):
        print(f"Fetching point-in-time history: batch {index}/{len(batches)}...", flush=True)
        try:
            frame = yf.download(
                list(batch),
                start=start.isoformat(),
                end=end.isoformat(),
                group_by="ticker",
                auto_adjust=True,
                actions=False,
                threads=True,
                progress=False,
                timeout=30,
            )
        except Exception as exc:
            print(f"  Batch failed: {exc}")
            continue
        for symbol in batch:
            stock_frame = ticker_frame(frame, symbol, len(batch))
            if stock_frame is not None and "Close" in stock_frame.columns:
                histories[symbol] = stock_frame.dropna(subset=["Close"])
    return histories


def load_point_in_time_rows(
    summary: pd.DataFrame, input_dir: Path
) -> tuple[list[dict[str, object]], list[str]]:
    cache: dict[Path, pd.DataFrame] = {}
    records: list[dict[str, object]] = []
    warnings: list[str] = []
    for row in summary.itertuples(index=False):
        signal_date = pd.Timestamp(getattr(row, "First_Penny_Date")).date()
        path = input_dir / f"penny_stock_scores_{signal_date:%d_%m_%y}.csv"
        try:
            if path not in cache:
                cache[path] = pd.read_csv(path)
            snapshot = cache[path]
            match = snapshot[snapshot["Symbol"].eq(getattr(row, "Symbol"))]
            if match.empty:
                warnings.append(f"{row.Symbol}: not found in {path.name}")
                continue
            record = match.iloc[0].to_dict()
            record.update(
                {
                    "Symbol": getattr(row, "Symbol"),
                    "Signal Date": signal_date,
                    "Entry Price": getattr(row, "First_Penny_Price"),
                    "Current Price": getattr(row, "Current_Price"),
                    "Current Price Date": getattr(row, "Current_Price_Date"),
                    "Holding Days": getattr(row, "Holding_Days_From_First"),
                    "Split-Adjusted Multiple": getattr(row, "Multiple_From_First"),
                    "Is Multibagger": bool(getattr(row, "First_Entry_Multibagger")),
                }
            )
            records.append(record)
        except (OSError, KeyError, pd.errors.ParserError) as exc:
            warnings.append(f"{row.Symbol}: {exc}")
    return records, warnings


def auc(labels: Sequence[bool], scores: Sequence[float]) -> float:
    positive = [score for label, score in zip(labels, scores) if label]
    negative = [score for label, score in zip(labels, scores) if not label]
    if not positive or not negative:
        return math.nan
    wins = 0.0
    for positive_score in positive:
        for negative_score in negative:
            wins += positive_score > negative_score
            wins += 0.5 * (positive_score == negative_score)
    return wins / (len(positive) * len(negative))


def average_precision(labels: Sequence[bool], scores: Sequence[float]) -> float:
    ordered = sorted(zip(scores, labels), reverse=True)
    positive_count = sum(labels)
    if positive_count == 0:
        return math.nan
    hits = 0
    precision_sum = 0.0
    for rank, (_, label) in enumerate(ordered, start=1):
        if label:
            hits += 1
            precision_sum += hits / rank
    return precision_sum / positive_count


def print_score_metrics(result: pd.DataFrame) -> None:
    labels = result["Is Multibagger"].astype(bool).tolist()
    for score_column, label in [
        ("Old Score (%)", "Old score"),
        ("V2 Potential Score", "V2 score"),
    ]:
        scores = result[score_column].astype(float).tolist()
        print(
            f"{label}: AUC={auc(labels, scores):.3f}, "
            f"average precision={average_precision(labels, scores):.3f}"
        )
        ordered = result.sort_values(score_column, ascending=False)
        counts = [
            f"top {size}: {int(ordered.head(size)['Is Multibagger'].sum())}"
            for size in (20, 50, 100)
        ]
        print("  " + ", ".join(counts))


def print_comparison(
    result: pd.DataFrame, candidate_score: float, mature_days: int
) -> None:
    labels = result["Is Multibagger"].astype(bool).tolist()
    positives = sum(labels)
    print()
    print(f"Resolved first-entry stocks: {len(result)} ({positives} multibaggers)")
    print_score_metrics(result)

    selected = result[
        (result["V2 Potential Score"] >= candidate_score)
        & result["Hard Gate Eligible"]
    ]
    selected_winners = int(selected["Is Multibagger"].sum())
    precision = selected_winners / len(selected) if len(selected) else 0
    recall = selected_winners / positives if positives else 0
    print(
        f"V2 >= {candidate_score:g}: {len(selected)} stocks, {selected_winners} winners, "
        f"precision={precision:.1%}, recall={recall:.1%}"
    )

    mature = result[pd.to_numeric(result["Holding Days"], errors="coerce") >= mature_days]
    mature_positives = int(mature["Is Multibagger"].sum())
    if mature_positives and mature_positives < len(mature):
        print()
        print(
            f"Mature subset (holding period >= {mature_days} days): "
            f"{len(mature)} stocks ({mature_positives} multibaggers)"
        )
        print_score_metrics(mature)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--input-dir", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=Path("backtest_results"))
    parser.add_argument("--candidate-score", type=float, default=CANDIDATE_SCORE)
    parser.add_argument("--mature-days", type=int, default=180)
    parser.add_argument("--batch-size", type=int, default=100)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 0 <= args.candidate_score <= 100:
        raise SystemExit("--candidate-score must be between 0 and 100")
    if args.mature_days < 0:
        raise SystemExit("--mature-days must not be negative")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    summary_path = args.summary or latest_summary(args.output_dir)
    summary = pd.read_csv(summary_path)
    summary = summary[pd.to_numeric(summary["Current Price"], errors="coerce").notna()].copy()
    summary["First Entry Multibagger"] = (
        summary["First Entry Multibagger"].astype(str).str.lower().eq("true")
    )
    summary["First Penny Date"] = pd.to_datetime(summary["First Penny Date"])
    summary.columns = [column.replace(" ", "_").replace("(%)", "Pct") for column in summary.columns]

    records, warnings = load_point_in_time_rows(summary, args.input_dir)
    for warning in warnings:
        print(f"warning: {warning}")
    if not records:
        print("No point-in-time records could be loaded.")
        return 1

    start = min(record["Signal Date"] for record in records) - timedelta(days=380)
    end = max(record["Signal Date"] for record in records) + timedelta(days=2)
    symbols = [str(record["Symbol"]) for record in records]
    histories = fetch_history(symbols, start, end, args.batch_size)

    output_rows: list[dict[str, object]] = []
    for record in records:
        symbol = str(record["Symbol"])
        signal_date = record["Signal Date"]
        history = histories.get(symbol)
        if history is not None:
            index = history.index
            if getattr(index, "tz", None) is not None:
                index = index.tz_localize(None)
                history = history.copy()
                history.index = index
            history = history.loc[index.date <= signal_date]
        market_features = calculate_market_features(history)
        metrics = dict(record)
        metrics.update(market_features)
        stock_for_scoring = {"quantitative": metrics, "qualitative": {}}
        _, v2_score, breakdown = future_multibagger_score(stock_for_scoring)
        gate_reasons = hard_gate_reasons(stock_for_scoring)
        row: dict[str, object] = {
            "Symbol": symbol,
            "Signal Date": signal_date.isoformat(),
            "Entry Price": record["Entry Price"],
            "Current Price": record["Current Price"],
            "Holding Days": record["Holding Days"],
            "Split-Adjusted Multiple": record["Split-Adjusted Multiple"],
            "Is Multibagger": record["Is Multibagger"],
            "Old Score (%)": record.get("Score (%)"),
            "V2 Potential Score": v2_score,
            "V2 Band": score_band(v2_score),
            "Hard Gate Eligible": not gate_reasons,
            "Hard Gate Reasons": "; ".join(gate_reasons),
        }
        row.update(market_features)
        row.update({f"{name} (v2)": value for name, value in breakdown.items()})
        output_rows.append(row)

    result = pd.DataFrame(output_rows)
    result["Old Rank"] = result["Old Score (%)"].rank(method="min", ascending=False).astype(int)
    result["V2 Rank"] = result["V2 Potential Score"].rank(method="min", ascending=False).astype(int)
    result = result.sort_values(["V2 Rank", "Symbol"])
    print_comparison(result, args.candidate_score, args.mature_days)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_date = report_date_from_path(summary_path)
    output_path = args.output_dir / f"multibagger_v2_backtest_{report_date}.csv"
    result.to_csv(output_path, index=False)
    print(f"Backtest rows saved to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
