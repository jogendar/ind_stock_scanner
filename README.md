# Indian Stock Scanner

## V2 future-potential scanner

`multibagger_v2.py` keeps the sub-Rs-20 universe but replaces the original
equal-weight factor score with a 100-point, explainable potential score:

| Pillar | Maximum |
|---|---:|
| Size runway | 15 |
| Promoter/ownership alignment | 10 |
| Fundamental growth | 20 |
| Financial survival and self-funding | 20 |
| Valuation sanity | 10 |
| Volume accumulation and trend confirmation | 25 |

The original downside checks retain their 15-point cap. Small additional
2-point penalties cover negative free cash flow, capital returns below 5%,
P/E above 50, EV/EBITDA above 30, and a persistent six- and twelve-month
downtrend. Audit qualifications and material dilution can add larger explicit
penalties when those fields are supplied; the combined deduction is capped at
30 points.

A stock is capped below the default candidate cutoff when three independent
fragility conditions occur together: negative free cash flow, capital returns
below 5%, and extreme valuation (P/E above 50 or EV/EBITDA above 30). This gate
prevents historical growth and a volume spike from overwhelming weak business
economics. Extremely valued stocks are also held out of ranked candidate lists
when both free cash flow and ROE/ROCE are unavailable, because the scanner
cannot run either core risk check. A known corporate-action window receives no
volume-accumulation points. Other missing values are not treated as failures
and earn no positive points.

Run the live NSE scan with:

```bash
python3 multibagger_v2.py
```

The scanner downloads the current NSE equity list, treats `EQ` and `BE` series
stocks identically for scoring, scores stocks whose current price is strictly
below Rs 20, and writes
`multibagger_v2_scores_DD_MM_YY.csv`. A score of 50 is the default watchlist
cutoff. Size affects the score but is not a hard filter; an optional ceiling can
be supplied with `--max-market-cap-cr`. Every result includes an `NSE Series`
column so trade-for-trade `BE` stocks remain visible without receiving a score
bonus or penalty.

```bash
python3 multibagger_v2.py \
  --price-threshold 20 \
  --candidate-score 50 \
  --max-market-cap-cr 5000
```

The scheduled GitHub workflow keeps the original V1 scanner as its primary
scan and commits `penny_stock_scores_DD_MM_YY.csv` in the repository root.
Afterward, V2 starts on a fresh runner and reuses that completed V1 snapshot
instead of making thousands of duplicate Yahoo fundamentals requests. It only
batch-fetches one year of market history for the sub-Rs-25 `EQ`/`BE` universe
and writes `v2data/multibagger_v2_scores_DD_MM_YY.csv`. If more than 10% of
those market histories are unavailable, V2 fails without committing a partial
file; the primary V1 result remains committed. If Screener.in repeatedly times out,
promoter-data requests pause for five minutes after three six-second failures;
this keeps a temporary upstream outage from exhausting GitHub's six-hour job
limit. The unavailable promoter fields remain missing and receive no points.
Manual dispatches also offer a `v2_only` recovery option that rebuilds V2 from
the current day's already committed V1 snapshot.

## Backtest stocks recorded below Rs 20

`backtest_multibaggers.py` reads only dated snapshots named
`penny_stock_scores_DD_MM_YY.csv`. It independently selects rows whose recorded
`Price` is strictly below Rs 20, fetches the latest Yahoo Finance close and
split history in batches, and writes:

- a one-row-per-stock summary based on the first qualifying snapshot; and
- an observation-level CSV for every qualifying stock/date pair.

Install the declared dependencies and run:

```bash
python3 -m pip install -r requirements.txt
python3 backtest_multibaggers.py
```

Results are saved under `backtest_results/`. The default multibagger definition
is a current price at least 2x the entry price. Both thresholds are configurable:

```bash
python3 backtest_multibaggers.py \
  --price-threshold 20 \
  --multibagger-threshold 2 \
  --start-date 2025-10-15
```

For an offline/reproducible run, pass `--prices-file prices.csv`. It must contain
`Symbol` and `Current Price`; `Price Date` and `Source` are optional.

The live report adjusts each entry for Yahoo Finance stock-split events before
classifying multibaggers, while retaining the raw multiple for audit. It does
not include dividends or trading costs. Ticker changes, mergers, and other
corporate actions still require manual review before drawing an investment
conclusion. Offline `--prices-file` runs cannot adjust splits unless the input
prices are already made comparable.

### V2 score backtest

After generating the split-adjusted outcome summary, compare the original and
v2 rankings with:

```bash
python3 backtest_score_v2.py
```

On the available 386 resolved first-entry stocks (17 current multibaggers), the
point-in-time backtest produced:

| Metric | Original | V2 |
|---|---:|---:|
| ROC AUC | 0.543 | 0.599 |
| Average precision | 0.052 | 0.100 |
| Winners in top 20 | 1 | 3 |
| Winners in top 50 | 1 | 5 |
| Winners in top 100 | 3 | 7 |

At the default score cutoff of 50, v2 selected 52 eligible stocks containing 6
of the 17 winners (11.5% precision and 35.3% recall). These are exploratory
results from only 15 October 2025 through 26 September 2026, with overlapping
holding periods and very few positive examples. They demonstrate ranking lift
on this archive, not validated future performance. A longer walk-forward test
is required before using the score for investment decisions.

Restricting the comparison to the 333 stocks with at least 180 holding days
still favored v2: AUC was 0.571 versus 0.525 and average precision was 0.113
versus 0.052.

As a targeted 300-day sanity check, the fixed V2 top 10 on 9 December 2025 had
an equal-weight return of 14.36% through 1 October 2026, with three stocks above
50% and one above 2x. The prior V2 top 10 returned 13.10% with the same hit
counts. HARDWYN and INDOWIND were removed by the compound-fragility gate. This
case motivated the safeguard and is therefore not an independent holdout; the
full-universe metrics above are the more useful regression check.

### V2 top-50 stability portfolio

`backtest_v2_portfolio.py` simulates buying a stock only after it has remained
in the daily V2 top 50 for 20 consecutive NSE trading snapshots. The purchase
uses the recorded price on the 20th snapshot, so the confirmation period does
not introduce look-ahead. Each symbol is purchased at most once.

```bash
python3 backtest_v2_portfolio.py \
  --top-n 50 \
  --stable-sessions 20 \
  --investment-per-stock 5000 \
  --valuation-date 2026-09-30
```

The output under `backtest_results/` includes the confirmation date and rank,
entry price, split-adjusted share count, latest price, current value, and
profit/loss for every position. Fractional shares are allowed. Dividends,
brokerage, taxes, and slippage are excluded. Any selected holding whose current
listing cannot be resolved is conservatively valued at zero in the portfolio
summary and remains marked `unresolved` in the CSV for manual ticker-change or
corporate-action review. `--valuation-date` excludes later or incomplete daily
candles and makes the valuation cutoff reproducible.

With the fixed score, the same 20-session/top-50 simulation through 30 September
2026 bought 88 stocks: Rs 440,000 became Rs 437,576.38, a -0.55% return, with
three multibaggers. The earlier V2 result was +4.93% with four multibaggers, but
its extra winner was SANGINITA, whose later takeover was not public or reliably
predictable at entry. Removing SANGINITA from the earlier portfolio changes its
return to -0.89%; on that comparable set, the fixed V2 is modestly better. This
is an explicit safety-versus-hindsight-return tradeoff, not evidence of a
profitable strategy.
