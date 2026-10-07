#!/usr/bin/env python3
"""Scan NSE stocks below Rs 20 with the explainable v2 potential score."""

from __future__ import annotations

import argparse
from datetime import datetime
import io
import math
from pathlib import Path
from typing import Sequence
import zipfile

import pandas as pd
import requests
import yfinance as yf
from curl_cffi.requests import Session

from data_fetcher import fetch_quantitative_data
from market_features import calculate_market_features
from score import multibagger_score_two_dim
from score_v2 import (
    CANDIDATE_SCORE,
    future_multibagger_score,
    hard_gate_reasons,
    score_band,
)


SUPPORTED_NSE_SERIES = frozenset({"EQ", "BE"})
DEFAULT_MAX_ERROR_RATE = 0.10


def finite_number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def download_equity_list(destination: Path) -> bool:
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        if "zip" in content_type:
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                member = next(
                    name for name in archive.namelist() if name.upper().endswith(".CSV")
                )
                destination.write_bytes(archive.read(member))
        else:
            destination.write_bytes(response.content)
        print(f"Updated equity list: {destination}")
        return True
    except (requests.RequestException, zipfile.BadZipFile, StopIteration, OSError) as exc:
        print(f"Could not update equity list: {exc}")
        return False


def load_securities(path: Path) -> list[tuple[str, str]]:
    frame = pd.read_csv(path)
    frame.columns = [str(column).strip() for column in frame.columns]
    if "SYMBOL" not in frame.columns:
        raise ValueError(f"{path} does not contain a SYMBOL column")
    if "SERIES" in frame.columns:
        frame["_NSE_SERIES"] = (
            frame["SERIES"].astype(str).str.strip().str.upper()
        )
        frame = frame[frame["_NSE_SERIES"].isin(SUPPORTED_NSE_SERIES)]
    else:
        frame["_NSE_SERIES"] = ""

    securities: list[tuple[str, str]] = []
    for symbol, series in zip(frame["SYMBOL"], frame["_NSE_SERIES"]):
        clean_symbol = str(symbol).strip()
        if clean_symbol:
            securities.append((clean_symbol, str(series).strip()))
    return securities


def load_symbols(path: Path) -> list[str]:
    """Return supported symbols while preserving the original public helper."""
    return [symbol for symbol, _ in load_securities(path)]


def normalized_ticker(symbol: object) -> str:
    ticker = str(symbol).strip().upper()
    return ticker if ticker.endswith(".NS") else f"{ticker}.NS"


def ticker_frame(
    frame: pd.DataFrame, symbol: str, symbol_count: int
) -> pd.DataFrame | None:
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


def fetch_market_histories(
    symbols: Sequence[str], batch_size: int
) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    batches = [
        symbols[offset : offset + batch_size]
        for offset in range(0, len(symbols), batch_size)
    ]
    for index, batch in enumerate(batches, start=1):
        print(f"Fetching current history: batch {index}/{len(batches)}...", flush=True)
        try:
            frame = yf.download(
                list(batch),
                period="1y",
                group_by="ticker",
                auto_adjust=True,
                actions=False,
                threads=True,
                progress=False,
                timeout=30,
            )
        except Exception as exc:
            print(f"  History batch failed: {exc}")
            continue
        for symbol in batch:
            stock_frame = ticker_frame(frame, symbol, len(batch))
            if stock_frame is not None and "Close" in stock_frame.columns:
                stock_frame = stock_frame.dropna(subset=["Close"])
                if not stock_frame.empty:
                    histories[symbol] = stock_frame
    return histories


def load_snapshot_candidates(
    path: Path,
    series_by_ticker: dict[str, str],
    price_threshold: float,
    max_market_cap_cr: float | None,
) -> list[dict[str, object]]:
    frame = pd.read_csv(path)
    if "Symbol" not in frame.columns or "Price" not in frame.columns:
        raise ValueError(f"{path} must contain Symbol and Price columns")

    records: list[dict[str, object]] = []
    for raw_record in frame.to_dict(orient="records"):
        ticker = normalized_ticker(raw_record.get("Symbol", ""))
        nse_series = series_by_ticker.get(ticker)
        price = finite_number(raw_record.get("Price"))
        if nse_series is None or price is None or price <= 0 or price >= price_threshold:
            continue

        market_cap = finite_number(raw_record.get("market_cap"))
        market_cap_cr = finite_number(raw_record.get("Market Cap (Cr)"))
        if market_cap is None and market_cap_cr is not None:
            market_cap = market_cap_cr * 10_000_000
        if market_cap_cr is None and market_cap is not None:
            market_cap_cr = market_cap / 10_000_000
        if market_cap is None or market_cap_cr is None:
            continue
        if max_market_cap_cr is not None and market_cap_cr > max_market_cap_cr:
            continue

        record = dict(raw_record)
        record.update(
            {
                "Symbol": ticker,
                "NSE Series": nse_series,
                "Price": price,
                "Market Cap (Cr)": market_cap_cr,
                "market_cap": market_cap,
                "stock_price": price,
            }
        )
        records.append(record)
    return records


def build_scored_result(
    ticker: str,
    nse_series: str,
    price: float,
    market_cap: float,
    metrics: dict[str, object],
    candidate_score: float,
    old_score: float | None,
    market_data_available: bool,
) -> dict[str, object]:
    stock_for_scoring = {"quantitative": metrics, "qualitative": {}}
    if old_score is None:
        _, old_score, _ = multibagger_score_two_dim(stock_for_scoring)
    points, potential_score, breakdown = future_multibagger_score(stock_for_scoring)
    gate_reasons = hard_gate_reasons(stock_for_scoring)
    hard_gate_eligible = not gate_reasons

    result: dict[str, object] = {
        "Symbol": ticker,
        "NSE Series": nse_series,
        "Price": price,
        "Market Cap (Cr)": market_cap / 10_000_000,
        "V2 Potential Score": potential_score,
        "V2 Band": score_band(potential_score),
        "Candidate": potential_score >= candidate_score and hard_gate_eligible,
        "Hard Gate Eligible": hard_gate_eligible,
        "Hard Gate Reasons": "; ".join(gate_reasons),
        "Market Data Available": market_data_available,
        "Penny_stock": True,
        "Old Score (%)": old_score,
    }
    source_only_columns = {"Score (%)", "Error", "NSE Series"}
    for name, value in metrics.items():
        if name not in result and name not in source_only_columns and not name.endswith(" (s)"):
            result[name] = value
    result.update({f"{name} (v2)": value for name, value in breakdown.items()})
    print(
        f"  {ticker}: Rs {price:.2f}, market cap {market_cap / 10_000_000:.1f} Cr, "
        f"v2={points:.1f} ({score_band(points)})"
    )
    return result


def score_snapshot_stock(
    record: dict[str, object],
    history: pd.DataFrame | None,
    candidate_score: float,
) -> dict[str, object]:
    ticker = normalized_ticker(record.get("Symbol", ""))
    price = finite_number(record.get("Price"))
    market_cap = finite_number(record.get("market_cap"))
    if price is None or market_cap is None:
        raise ValueError(f"{ticker} is missing price or market cap in the V1 snapshot")

    metrics = dict(record)
    metrics["stock_price"] = price
    metrics["market_cap"] = market_cap
    metrics.update(calculate_market_features(history))
    return build_scored_result(
        ticker=ticker,
        nse_series=str(record.get("NSE Series", "")),
        price=price,
        market_cap=market_cap,
        metrics=metrics,
        candidate_score=candidate_score,
        old_score=finite_number(record.get("Score (%)")),
        market_data_available=history is not None and not history.empty,
    )


def scan_stock(
    symbol: str,
    session: Session,
    price_threshold: float,
    candidate_score: float,
    max_market_cap_cr: float | None,
    nse_series: str = "",
) -> dict[str, object] | None:
    ticker = f"{symbol}.NS"
    stock = yf.Ticker(ticker, session=session)
    info = stock.info
    price = finite_number(info.get("regularMarketPrice", info.get("currentPrice")))
    market_cap = finite_number(info.get("marketCap"))
    if price is None or market_cap is None:
        print(f"  Skipping {ticker}: price or market cap is unavailable.")
        return None
    if price <= 0 or price >= price_threshold:
        return None
    market_cap_cr = market_cap / 10_000_000
    if max_market_cap_cr is not None and market_cap_cr > max_market_cap_cr:
        return None

    quantitative_data = fetch_quantitative_data(ticker)
    metrics = quantitative_data.get("metrics", {})
    metrics["stock_price"] = price
    metrics["market_cap"] = market_cap

    history: pd.DataFrame | None = None
    try:
        history = stock.history(period="1y", auto_adjust=True)
        metrics.update(calculate_market_features(history))
    except Exception as exc:
        print(f"  Warning: market-history features unavailable for {ticker}: {exc}")
        metrics.update(calculate_market_features(None))

    return build_scored_result(
        ticker=ticker,
        nse_series=nse_series,
        price=price,
        market_cap=market_cap,
        metrics=metrics,
        candidate_score=candidate_score,
        old_score=None,
        market_data_available=history is not None and not history.empty,
    )


def error_rate_exceeded(errors: int, total: int, maximum: float) -> bool:
    return total > 0 and errors / total > maximum


def save_results(
    results: list[dict[str, object]], args: argparse.Namespace, errors: int
) -> int:
    if not results:
        print("No stocks met the price and market-cap filters.")
        return 1
    output = pd.DataFrame(results).sort_values(
        ["Candidate", "V2 Potential Score", "Symbol"],
        ascending=[False, False, True],
    )
    output_path = args.output or Path(
        f"multibagger_v2_scores_{datetime.now():%d_%m_%y}.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)
    candidate_count = int(output["Candidate"].sum())
    print()
    print(f"Sub-Rs-{args.price_threshold:g} stocks scored: {len(output)}")
    print(f"Candidates at score >= {args.candidate_score:g}: {candidate_count}")
    print(f"Per-symbol errors: {errors}")
    print(f"Results saved to {output_path}")
    return 0


def run_snapshot_scanner(
    args: argparse.Namespace, securities: list[tuple[str, str]]
) -> int:
    if not args.source_v1_file.exists():
        print(f"Error: V1 source snapshot not found: {args.source_v1_file}")
        return 1
    series_by_ticker = {
        normalized_ticker(symbol): nse_series for symbol, nse_series in securities
    }
    try:
        records = load_snapshot_candidates(
            args.source_v1_file,
            series_by_ticker,
            args.price_threshold,
            args.max_market_cap_cr,
        )
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        print(f"Error reading V1 source snapshot: {exc}")
        return 1
    if args.limit is not None:
        records = records[: args.limit]
    print(
        f"Loaded {len(records)} sub-Rs-{args.price_threshold:g} EQ/BE stocks "
        f"from {args.source_v1_file}."
    )
    if not records:
        return 1

    symbols = [str(record["Symbol"]) for record in records]
    histories = fetch_market_histories(symbols, args.history_batch_size)
    missing_histories = len(symbols) - len(histories)
    missing_rate = missing_histories / len(symbols)
    print(
        f"Market histories available: {len(histories)}/{len(symbols)} "
        f"({missing_rate:.1%} missing)."
    )
    if error_rate_exceeded(missing_histories, len(symbols), args.max_error_rate):
        print(
            "Error: market-history failure rate exceeds "
            f"{args.max_error_rate:.1%}; refusing to write a partial V2 file."
        )
        return 2

    results = [
        score_snapshot_stock(
            record,
            histories.get(str(record["Symbol"])),
            args.candidate_score,
        )
        for record in records
    ]
    return save_results(results, args, missing_histories)


def run_scanner(args: argparse.Namespace) -> int:
    print(f"--- Starting v2 scan at {datetime.now():%Y-%m-%d %H:%M:%S} ---")
    if not args.skip_equity_download and not download_equity_list(args.equity_file):
        print("Equity-list download failed; using the existing local file.")
    if not args.equity_file.exists():
        print(f"Error: equity list not found: {args.equity_file}")
        return 1
    try:
        securities = load_securities(args.equity_file)
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        print(f"Error reading equity list: {exc}")
        return 1
    if args.source_v1_file is not None:
        return run_snapshot_scanner(args, securities)
    if args.limit is not None:
        securities = securities[: args.limit]
    print(f"Loaded {len(securities)} EQ/BE-series symbols.")

    session = Session(impersonate="chrome110")
    results: list[dict[str, object]] = []
    errors = 0
    for index, (symbol, nse_series) in enumerate(securities, start=1):
        print(f"[{index}/{len(securities)}] {symbol} ({nse_series or 'unknown series'})")
        try:
            result = scan_stock(
                symbol,
                session,
                args.price_threshold,
                args.candidate_score,
                args.max_market_cap_cr,
                nse_series=nse_series,
            )
            if result is not None:
                results.append(result)
        except Exception as exc:
            errors += 1
            print(f"  Error processing {symbol}.NS: {exc}")

    if error_rate_exceeded(errors, len(securities), args.max_error_rate):
        print(
            f"Error: per-symbol failure rate {errors / len(securities):.1%} exceeds "
            f"{args.max_error_rate:.1%}; refusing to write a partial V2 file."
        )
        return 2
    return save_results(results, args, errors)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity-file", type=Path, default=Path("EQUITY_L.csv"))
    parser.add_argument("--price-threshold", type=float, default=20.0)
    parser.add_argument("--candidate-score", type=float, default=CANDIDATE_SCORE)
    parser.add_argument(
        "--max-market-cap-cr",
        type=float,
        help="Optional hard market-cap ceiling; by default size only affects scoring.",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--source-v1-file",
        type=Path,
        help="Reuse a completed V1 snapshot instead of refetching fundamentals.",
    )
    parser.add_argument("--history-batch-size", type=int, default=100)
    parser.add_argument("--max-error-rate", type=float, default=DEFAULT_MAX_ERROR_RATE)
    parser.add_argument("--limit", type=int, help="Process only the first N symbols.")
    parser.add_argument("--skip-equity-download", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.price_threshold <= 0:
        raise SystemExit("--price-threshold must be positive")
    if not 0 <= args.candidate_score <= 100:
        raise SystemExit("--candidate-score must be between 0 and 100")
    if args.max_market_cap_cr is not None and args.max_market_cap_cr <= 0:
        raise SystemExit("--max-market-cap-cr must be positive")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.history_batch_size <= 0:
        raise SystemExit("--history-batch-size must be positive")
    if not 0 <= args.max_error_rate <= 1:
        raise SystemExit("--max-error-rate must be between 0 and 1")
    return run_scanner(args)


if __name__ == "__main__":
    raise SystemExit(main())
