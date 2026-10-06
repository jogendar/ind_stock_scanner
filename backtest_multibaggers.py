#!/usr/bin/env python3
"""Backtest low-priced stock snapshots against the latest market close.

The dated input files are expected to use this naming convention:
``penny_stock_scores_DD_MM_YY.csv``.  Rows are selected from the recorded
``Price`` column rather than the historical ``Penny_stock`` flag because that
flag was produced with a different cutoff in older scanner versions.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math
from pathlib import Path
import re
import sys
from typing import Iterator, Sequence


SNAPSHOT_RE = re.compile(r"^penny_stock_scores_(\d{2})_(\d{2})_(\d{2})\.csv$")


@dataclass(frozen=True)
class Observation:
    symbol: str
    snapshot_date: date
    entry_price: float
    score: float | None
    source_file: str


@dataclass(frozen=True)
class LatestPrice:
    symbol: str
    price: float
    price_date: date | None
    source: str


@dataclass(frozen=True)
class SplitEvent:
    event_date: date
    factor: float


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def snapshot_date(path: Path) -> date | None:
    match = SNAPSHOT_RE.match(path.name)
    if not match:
        return None
    day, month, short_year = (int(part) for part in match.groups())
    try:
        return date(2000 + short_year, month, day)
    except ValueError:
        return None


def discover_snapshots(
    input_dir: Path,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[tuple[date, Path]]:
    snapshots: list[tuple[date, Path]] = []
    for path in input_dir.glob("penny_stock_scores_*.csv"):
        observed_on = snapshot_date(path)
        if observed_on is None:
            continue
        if start_date is not None and observed_on < start_date:
            continue
        if end_date is not None and observed_on > end_date:
            continue
        snapshots.append((observed_on, path))
    return sorted(snapshots, key=lambda item: (item[0], item[1].name))


def finite_float(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def normalize_symbol(symbol: str, exchange_suffix: str) -> str:
    normalized = symbol.strip().upper()
    if normalized and "." not in normalized and exchange_suffix:
        normalized += exchange_suffix
    return normalized


def read_observations(
    snapshots: Sequence[tuple[date, Path]],
    price_threshold: float,
    exchange_suffix: str,
) -> tuple[list[Observation], list[str]]:
    observations: list[Observation] = []
    warnings: list[str] = []

    for observed_on, path in snapshots:
        try:
            with path.open("r", newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                columns = set(reader.fieldnames or [])
                missing = {"Symbol", "Price"} - columns
                if missing:
                    warnings.append(
                        f"{path.name}: skipped; missing column(s) "
                        + ", ".join(sorted(missing))
                    )
                    continue

                for row in reader:
                    symbol = normalize_symbol(row.get("Symbol", ""), exchange_suffix)
                    entry_price = finite_float(row.get("Price"))
                    if (
                        not symbol
                        or entry_price is None
                        or entry_price <= 0
                        or entry_price >= price_threshold
                    ):
                        continue
                    observations.append(
                        Observation(
                            symbol=symbol,
                            snapshot_date=observed_on,
                            entry_price=entry_price,
                            score=finite_float(row.get("Score (%)")),
                            source_file=path.name,
                        )
                    )
        except (OSError, csv.Error) as exc:
            warnings.append(f"{path.name}: skipped; {exc}")

    observations.sort(key=lambda item: (item.snapshot_date, item.symbol))
    return observations, warnings


def chunks(values: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _series_latest_price(series: object, symbol: str) -> LatestPrice | None:
    try:
        clean = series.dropna()  # type: ignore[attr-defined]
    except AttributeError:
        return None
    if clean.empty:
        return None

    price = finite_float(clean.iloc[-1])
    if price is None or price <= 0:
        return None
    timestamp = clean.index[-1]
    try:
        price_date = timestamp.date()
    except AttributeError:
        try:
            price_date = date.fromisoformat(str(timestamp)[:10])
        except ValueError:
            price_date = None
    return LatestPrice(symbol, price, price_date, "Yahoo Finance last close")


def _ticker_frame(frame: object, symbol: str, symbol_count: int) -> object | None:
    columns = frame.columns  # type: ignore[attr-defined]
    levels = getattr(columns, "nlevels", 1)
    if symbol_count == 1 and levels == 1:
        return frame
    if levels <= 1:
        return None

    level_zero = set(columns.get_level_values(0))
    level_one = set(columns.get_level_values(1))
    if symbol in level_zero:
        return frame[symbol]  # type: ignore[index]
    if symbol in level_one:
        try:
            return frame.xs(symbol, axis=1, level=1)  # type: ignore[attr-defined]
        except (KeyError, ValueError):
            return None
    return None


def _extract_download_market_data(
    frame: object, symbols: Sequence[str]
) -> tuple[dict[str, LatestPrice], dict[str, list[SplitEvent]]]:
    """Extract closes and split events from either yfinance download shape."""
    prices: dict[str, LatestPrice] = {}
    splits: dict[str, list[SplitEvent]] = {}
    if frame is None or getattr(frame, "empty", True):
        return prices, splits

    for symbol in symbols:
        ticker_frame = _ticker_frame(frame, symbol, len(symbols))
        if ticker_frame is None:
            continue
        ticker_columns = ticker_frame.columns  # type: ignore[attr-defined]
        if "Close" in ticker_columns:
            latest = _series_latest_price(ticker_frame["Close"], symbol)  # type: ignore[index]
            if latest:
                prices[symbol] = latest
        if "Stock Splits" not in ticker_columns:
            continue
        events: list[SplitEvent] = []
        split_series = ticker_frame["Stock Splits"].dropna()  # type: ignore[index]
        for timestamp, raw_factor in split_series.items():
            factor = finite_float(raw_factor)
            if factor is None or factor <= 0:
                continue
            try:
                event_date = timestamp.date()
            except AttributeError:
                try:
                    event_date = date.fromisoformat(str(timestamp)[:10])
                except ValueError:
                    continue
            events.append(SplitEvent(event_date, factor))
        if events:
            splits[symbol] = sorted(events, key=lambda event: event.event_date)
    return prices, splits


def fetch_market_data(
    symbols: Sequence[str], batch_size: int, history_start: date
) -> tuple[dict[str, LatestPrice], dict[str, list[SplitEvent]]]:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed; run `python3 -m pip install -r requirements.txt` "
            "or provide --prices-file"
        ) from exc

    prices: dict[str, LatestPrice] = {}
    splits: dict[str, list[SplitEvent]] = {}
    batches = list(chunks(symbols, batch_size))
    for batch_number, batch in enumerate(batches, start=1):
        print(
            f"Fetching prices: batch {batch_number}/{len(batches)} "
            f"({len(batch)} symbols)...",
            flush=True,
        )
        try:
            frame = yf.download(
                tickers=list(batch),
                start=history_start.isoformat(),
                end=(datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat(),
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                actions=True,
                threads=True,
                progress=False,
                timeout=30,
            )
        except Exception as exc:  # yfinance raises several transport exceptions
            print(f"  Batch failed: {exc}", file=sys.stderr)
            continue
        batch_prices, batch_splits = _extract_download_market_data(frame, batch)
        prices.update(batch_prices)
        splits.update(batch_splits)
    return prices, splits


def read_prices_file(path: Path, exchange_suffix: str) -> dict[str, LatestPrice]:
    prices: dict[str, LatestPrice] = {}
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = {"Symbol", "Current Price"} - columns
        if missing:
            raise ValueError(
                f"{path}: missing required column(s): {', '.join(sorted(missing))}"
            )
        for row in reader:
            symbol = normalize_symbol(row.get("Symbol", ""), exchange_suffix)
            price = finite_float(row.get("Current Price"))
            if not symbol or price is None or price <= 0:
                continue
            raw_date = (row.get("Price Date") or "").strip()
            try:
                price_date = date.fromisoformat(raw_date) if raw_date else None
            except ValueError:
                price_date = None
            source = (row.get("Source") or "prices file").strip()
            prices[symbol] = LatestPrice(symbol, price, price_date, source)
    return prices


def rounded(value: float | None, places: int = 4) -> float | str:
    return "" if value is None else round(value, places)


def split_factor_since(
    events: Sequence[SplitEvent], entry_date: date, price_date: date | None
) -> float:
    factor = 1.0
    for event in events:
        if event.event_date > entry_date and (
            price_date is None or event.event_date <= price_date
        ):
            factor *= event.factor
    return factor


def result_metrics(
    entry_price: float,
    entry_date: date,
    current: LatestPrice | None,
    multibagger_threshold: float,
    split_events: Sequence[SplitEvent] = (),
) -> dict[str, object]:
    if current is None:
        return {
            "Current Price": "",
            "Current Price Date": "",
            "Current Price Source": "unresolved",
            "Holding Days": "",
            "Split Factor Since Entry": "",
            "Split-Adjusted Entry Price": "",
            "Raw Price Multiple": "",
            "Split-Adjusted Price Multiple": "",
            "Return (%)": "",
            "Is Multibagger": "",
        }

    split_factor = split_factor_since(split_events, entry_date, current.price_date)
    adjusted_entry_price = entry_price / split_factor
    raw_multiple = current.price / entry_price
    adjusted_multiple = current.price / adjusted_entry_price
    holding_days = (
        (current.price_date - entry_date).days if current.price_date is not None else ""
    )
    return {
        "Current Price": rounded(current.price),
        "Current Price Date": current.price_date.isoformat() if current.price_date else "",
        "Current Price Source": current.source,
        "Holding Days": holding_days,
        "Split Factor Since Entry": rounded(split_factor, 8),
        "Split-Adjusted Entry Price": rounded(adjusted_entry_price),
        "Raw Price Multiple": rounded(raw_multiple),
        "Split-Adjusted Price Multiple": rounded(adjusted_multiple),
        "Return (%)": rounded((adjusted_multiple - 1) * 100, 2),
        "Is Multibagger": adjusted_multiple >= multibagger_threshold,
    }


def build_observation_rows(
    observations: Sequence[Observation],
    latest_prices: dict[str, LatestPrice],
    multibagger_threshold: float,
    split_events: dict[str, list[SplitEvent]] | None = None,
) -> list[dict[str, object]]:
    split_events = split_events or {}
    rows: list[dict[str, object]] = []
    for observation in observations:
        row: dict[str, object] = {
            "Symbol": observation.symbol,
            "Snapshot Date": observation.snapshot_date.isoformat(),
            "Entry Price": rounded(observation.entry_price),
            "Score (%)": rounded(observation.score, 2),
            "Source File": observation.source_file,
        }
        row.update(
            result_metrics(
                observation.entry_price,
                observation.snapshot_date,
                latest_prices.get(observation.symbol),
                multibagger_threshold,
                split_events.get(observation.symbol, []),
            )
        )
        rows.append(row)
    return rows


def build_summary_rows(
    observations: Sequence[Observation],
    latest_prices: dict[str, LatestPrice],
    multibagger_threshold: float,
    split_events: dict[str, list[SplitEvent]] | None = None,
) -> list[dict[str, object]]:
    split_events = split_events or {}
    grouped: dict[str, list[Observation]] = {}
    for observation in observations:
        grouped.setdefault(observation.symbol, []).append(observation)

    rows: list[dict[str, object]] = []
    for symbol, symbol_observations in grouped.items():
        ordered = sorted(symbol_observations, key=lambda item: item.snapshot_date)
        first = ordered[0]
        latest = ordered[-1]
        lowest = min(ordered, key=lambda item: (item.entry_price, item.snapshot_date))
        scored = [item for item in ordered if item.score is not None]
        highest_score = max(
            scored,
            key=lambda item: (item.score if item.score is not None else -math.inf),
            default=None,
        )
        current = latest_prices.get(symbol)
        first_metrics = result_metrics(
            first.entry_price,
            first.snapshot_date,
            current,
            multibagger_threshold,
            split_events.get(symbol, []),
        )
        lowest_metrics = result_metrics(
            lowest.entry_price,
            lowest.snapshot_date,
            current,
            multibagger_threshold,
            split_events.get(symbol, []),
        )
        row: dict[str, object] = {
            "Symbol": symbol,
            "First Penny Date": first.snapshot_date.isoformat(),
            "First Penny Price": rounded(first.entry_price),
            "First Score (%)": rounded(first.score, 2),
            "Lowest Penny Date": lowest.snapshot_date.isoformat(),
            "Lowest Penny Price": rounded(lowest.entry_price),
            "Highest Score Date": (
                highest_score.snapshot_date.isoformat() if highest_score else ""
            ),
            "Highest Score (%)": rounded(highest_score.score, 2) if highest_score else "",
            "Latest Penny Date": latest.snapshot_date.isoformat(),
            "Latest Penny Price": rounded(latest.entry_price),
            "Penny Observations": len(ordered),
            "Current Price": first_metrics["Current Price"],
            "Current Price Date": first_metrics["Current Price Date"],
            "Current Price Source": first_metrics["Current Price Source"],
            "Holding Days From First": first_metrics["Holding Days"],
            "Split Factor From First": first_metrics["Split Factor Since Entry"],
            "Adjusted First Entry Price": first_metrics["Split-Adjusted Entry Price"],
            "Raw Multiple From First": first_metrics["Raw Price Multiple"],
            "Multiple From First": first_metrics["Split-Adjusted Price Multiple"],
            "Return From First (%)": first_metrics["Return (%)"],
            "First Entry Multibagger": first_metrics["Is Multibagger"],
            # The lowest-price view is useful, but it is not a tradable signal because
            # choosing the minimum requires future knowledge.
            "Split Factor From Lowest (Hindsight)": lowest_metrics[
                "Split Factor Since Entry"
            ],
            "Adjusted Lowest Entry Price (Hindsight)": lowest_metrics[
                "Split-Adjusted Entry Price"
            ],
            "Raw Multiple From Lowest (Hindsight)": lowest_metrics[
                "Raw Price Multiple"
            ],
            "Multiple From Lowest (Hindsight)": lowest_metrics[
                "Split-Adjusted Price Multiple"
            ],
            "Return From Lowest (%) (Hindsight)": lowest_metrics["Return (%)"],
            "Lowest Entry Multibagger (Hindsight)": lowest_metrics["Is Multibagger"],
        }
        rows.append(row)

    def sort_key(row: dict[str, object]) -> tuple[bool, float, str]:
        multiple = row["Multiple From First"]
        return (
            multiple == "",
            -(float(multiple) if multiple != "" else 0.0),
            str(row["Symbol"]),
        )

    return sorted(rows, key=sort_key)


def write_csv(path: Path, rows: Sequence[dict[str, object]]) -> None:
    if not rows:
        raise ValueError(f"cannot write {path}: there are no rows")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def print_report(
    snapshots: Sequence[tuple[date, Path]],
    observations: Sequence[Observation],
    summary_rows: Sequence[dict[str, object]],
) -> None:
    resolved = [row for row in summary_rows if row["Current Price"] != ""]
    multibaggers = [row for row in resolved if row["First Entry Multibagger"] is True]
    hindsight_multibaggers = [
        row for row in resolved if row["Lowest Entry Multibagger (Hindsight)"] is True
    ]
    print()
    print(f"Snapshots analyzed: {len(snapshots)}")
    print(
        f"Snapshot range: {snapshots[0][0].isoformat()} to "
        f"{snapshots[-1][0].isoformat()}"
    )
    print(f"Under-threshold observations: {len(observations)}")
    print(f"Unique stocks: {len(summary_rows)}")
    print(f"Latest prices resolved: {len(resolved)}/{len(summary_rows)}")
    print(f"Multibaggers from first qualifying snapshot: {len(multibaggers)}")
    print(f"Multibaggers from lowest observed price (hindsight): {len(hindsight_multibaggers)}")
    if multibaggers:
        print("\nTop multibaggers from first qualifying snapshot:")
        for row in multibaggers[:20]:
            print(
                f"  {row['Symbol']:<18} {float(row['Multiple From First']):>7.2f}x  "
                f"Rs {float(row['First Penny Price']):.2f} -> "
                f"Rs {float(row['Current Price']):.2f}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Find stocks recorded below a price threshold in dated score files and "
            "compare them with the latest market close."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=Path("backtest_results"))
    parser.add_argument("--price-threshold", type=float, default=20.0)
    parser.add_argument("--multibagger-threshold", type=float, default=2.0)
    parser.add_argument("--start-date", type=parse_iso_date)
    parser.add_argument("--end-date", type=parse_iso_date)
    parser.add_argument(
        "--prices-file",
        type=Path,
        help=(
            "Optional offline CSV with Symbol and Current Price columns; optional "
            "Price Date and Source columns."
        ),
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--exchange-suffix",
        default=".NS",
        help="Suffix added to bare symbols (default: .NS)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.price_threshold <= 0:
        print("error: --price-threshold must be positive", file=sys.stderr)
        return 2
    if args.multibagger_threshold <= 1:
        print("error: --multibagger-threshold must be greater than 1", file=sys.stderr)
        return 2
    if args.batch_size <= 0:
        print("error: --batch-size must be positive", file=sys.stderr)
        return 2
    if args.start_date and args.end_date and args.start_date > args.end_date:
        print("error: --start-date must not be after --end-date", file=sys.stderr)
        return 2

    snapshots = discover_snapshots(args.input_dir, args.start_date, args.end_date)
    if not snapshots:
        print(
            f"error: no dated penny_stock_scores_DD_MM_YY.csv files found in "
            f"{args.input_dir}",
            file=sys.stderr,
        )
        return 1

    print(f"Reading {len(snapshots)} snapshots...", flush=True)
    observations, warnings = read_observations(
        snapshots, args.price_threshold, args.exchange_suffix
    )
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    if not observations:
        print(
            f"No rows had a valid recorded price below Rs {args.price_threshold:g}.",
            file=sys.stderr,
        )
        return 1

    symbols = sorted({observation.symbol for observation in observations})
    try:
        if args.prices_file:
            latest_prices = read_prices_file(args.prices_file, args.exchange_suffix)
            split_events: dict[str, list[SplitEvent]] = {}
        else:
            history_start = min(item.snapshot_date for item in observations) - timedelta(
                days=7
            )
            latest_prices, split_events = fetch_market_data(
                symbols, args.batch_size, history_start
            )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    observation_rows = build_observation_rows(
        observations, latest_prices, args.multibagger_threshold, split_events
    )
    summary_rows = build_summary_rows(
        observations, latest_prices, args.multibagger_threshold, split_events
    )

    price_dates = [item.price_date for item in latest_prices.values() if item.price_date]
    report_date = max(price_dates) if price_dates else datetime.now(timezone.utc).date()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / f"multibagger_summary_{report_date.isoformat()}.csv"
    observations_path = (
        args.output_dir / f"multibagger_observations_{report_date.isoformat()}.csv"
    )
    write_csv(summary_path, summary_rows)
    write_csv(observations_path, observation_rows)

    print_report(snapshots, observations, summary_rows)
    print(f"\nSummary CSV: {summary_path}")
    print(f"Observation CSV: {observations_path}")
    if args.prices_file:
        print(
            "Note: offline prices do not include automatic split adjustment. Input "
            "prices must already be comparable."
        )
    else:
        print(
            "Note: classifications adjust for Yahoo Finance split events. They do not "
            "include dividends or trading costs; ticker changes and other corporate "
            "actions still require manual review."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
