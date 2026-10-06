# V2 Final AI Filter

Use this review **after V2 produces its ranked candidate list**. V2 is the
quantitative screen; this is a point-in-time sanity check intended to remove
false positives caused by temporary news spikes, stale growth figures, weak
capital efficiency, unsuitable sector ratios, or poor trading liquidity.

Do not silently modify the V2 score. Return a separate final verdict:

- **PASS** — suitable for the final watchlist.
- **WAIT** — potentially valid, but needs a cooling-off period or missing data.
- **REJECT** — the apparent opportunity is contradicted by material evidence.

## Review rules

1. For a current scan, use the newest information available at review time.
2. For a backtest, use only information published on or before the signal date.
   Never use later results to change the historical verdict.
3. Prefer dated primary sources: NSE/BSE filings, company results, annual
   reports, shareholding filings, and regulatory announcements. Include links
   and publication dates in the answer.
4. Treat missing data as uncertainty, not as good performance.
5. Do not pass or reject a company solely because its share price subsequently
   rose or fell.

## 1. Detect event-driven price and volume spikes

Search exchange filings and company announcements from at least 60 calendar
days before the signal. Identify licences, mergers, acquisitions, large orders,
takeovers, relistings, stock splits, bonuses, rights issues, preferential
allotments, regulatory approvals, and unusual-result announcements.

Flag an **event spike** when one or more of these is present:

- Price increased at least 40% over three sessions or 50% over five sessions.
- Any day's volume exceeded 10 times the previous 60-session median.
- The three largest-volume days supplied more than 60% of total 20-session
  volume.
- Most of the V2 volume ratio came from one to three sessions around a filing.

Then check follow-through:

- If the price is already more than 10% below the post-event peak, treat the
  move as a possible reversal rather than accumulation.
- Do not award confidence merely because 20-day average volume is high. Also
  inspect median volume with the largest three days removed.
- A licence or approval enables future business; it is not proof that revenue
  or profit has already increased.

**Decision:** normally return **WAIT** until at least 20 trading sessions have
passed with stable price, ordinary volume and no sharp reversal. Return
**REJECT** when the event spike is reversing and earnings do not support the
new valuation.

## 2. Give recent earnings priority over historical CAGR

Compare the latest quarter, trailing twelve months and latest financial year
with their prior comparable periods. Check revenue, operating profit, PAT,
EPS, operating margin and cash generation.

Major earnings warnings include:

- Latest quarterly or annual PAT declined more than 20%.
- Revenue increased while operating profit, PAT or margin declined materially.
- Latest annual profit declined despite a high three-year or five-year CAGR.
- Growth came from other income, asset sales, fair-value gains, exceptional
  items, or a very low comparison base.
- Quarterly performance is highly uneven and the latest result reverses the
  earlier growth trend.

Historical CAGR must not override a clear recent deterioration. A decline may
be excused only when an official filing identifies a genuinely one-off item and
the underlying operating result remains healthy.

## 3. Require meaningful capital efficiency

Review both ROE and ROCE using correctly calculated values:

- Below 5%: severe warning.
- From 5% to below 8%: weak; it cannot support a high-conviction multibagger
  verdict by itself.
- From 8% to below 12%: acceptable only with a clear improving trend.
- At least 12% and improving: preferred.

Do not allow a company at 5% to 8% to escape review merely because it is just
above V2's 5% penalty threshold. Relate valuation to returns: a P/E near 20 to
25 is not automatically cheap when ROE and ROCE are only 5% to 8%.

## 4. Verify cash generation and use sector-appropriate measures

- Missing free cash flow is not positive evidence. Mark confidence as low until
  cash-flow statements can be verified.
- Compare operating cash flow with PAT over several years and investigate a
  persistent gap.
- For banks, NBFCs, brokers and other financial companies, do not rely heavily
  on generic current ratio, debt/equity or free-cash-flow rules. Review the
  sector-appropriate measures available in filings, such as capital adequacy,
  asset quality, client assets, brokerage activity, regulatory capital and the
  quality of fee income.
- Recalculate quality where the V2 inputs are inappropriate for the sector.

## 5. Check normal liquidity, not headline volume

Calculate median traded value and volume after excluding the three largest
sessions.

Flag the candidate when:

- Ordinary daily turnover is too low to enter and exit without material impact.
- Volume falls more than 80% after the event window.
- Bid/ask behaviour, repeated circuits or many zero/very-low-volume sessions
  indicate that the quoted price may not be executable.
- Institutional ownership is negligible and the apparent liquidity is
  concentrated in a few sessions.

Stable promoter holding is useful, but it does not prove market demand. High
promoter ownership alone must never override weak earnings or liquidity.

## 6. Review volatility, drawdown and trend quality

Raise a major risk flag when at least two of the following are true:

- Three-month annualised volatility exceeds 60%.
- One-year maximum drawdown is worse than -40%.
- Fewer than 45% of the previous 63 sessions were positive.
- Price is more than 30% below its 52-week high.
- Price has fallen more than 10% from a recent event-driven peak.

This combination describes unstable speculation more often than persistent
institutional accumulation. A recent rebound should not erase the longer-term
drawdown.

## 7. Check merger and accounting comparability

If a merger, demerger, acquisition, change of business, restatement or major
subsidiary consolidation occurred during the growth-measurement period:

- Do not accept the reported CAGR without reconstructing comparable periods.
- Separate organic growth from the change in reporting scope.
- Check per-share growth and any increase in share count.
- Confirm whether the catalyst has produced operating income or only completed
  a legal/regulatory step.

If comparable numbers cannot be reconstructed, return **WAIT** or lower the
confidence; do not treat the CAGR as established business growth.

## 8. Recheck ownership and governance

- Verify current promoter holding and the change from the preceding quarter.
- Check promoter pledges, dilution, warrants, related-party transactions,
  auditor qualifications, regulatory actions and unexplained resignations.
- Unchanged promoter holding is neutral-to-positive, not evidence of promoter
  buying.
- A material promoter reduction, new pledge, audit qualification or unexplained
  dilution is normally a **REJECT** unless a primary filing clearly resolves it.

## Final decision logic

Return **REJECT** when any hard governance issue is present, or when at least
two independent major warnings are present, especially:

- failed event spike;
- recent earnings deterioration;
- weak capital efficiency;
- unreliable/missing cash evidence;
- abnormal or disappearing liquidity;
- merger-distorted growth without comparable figures.

Return **WAIT** when the thesis may be valid but:

- fewer than 20 sessions have passed since a major event;
- the price has not yet formed a stable base;
- a current result or cash-flow statement is unavailable; or
- accounting comparability cannot yet be established.

Return **PASS** only when:

- price and volume confirmation persists beyond the event window;
- recent operating profit and PAT are stable or improving;
- capital efficiency is adequate and preferably improving;
- normal-session liquidity is investable;
- growth is comparable and not mainly an accounting/event effect; and
- no material governance warning remains unresolved.

## Required AI output

For each V2 candidate, return this compact record:

```text
Symbol:
V2 rank and score:
Final verdict: PASS | WAIT | REJECT
Confidence: High | Medium | Low
Event/catalyst and date:
Price/volume spike: Yes | No
Recent earnings trend:
ROE / ROCE:
Cash-flow evidence:
Normal liquidity:
Ownership/governance:
Merger/accounting comparability:
Major warning codes:
Reason in 2-4 sentences:
Primary sources with dates:
```

Use these warning codes where applicable:

`EVENT_SPIKE`, `FAILED_FOLLOW_THROUGH`, `RECENT_PAT_DECLINE`,
`MARGIN_COMPRESSION`, `WEAK_CAPITAL_RETURNS`, `MISSING_CASH_EVIDENCE`,
`ABNORMAL_LIQUIDITY`, `HIGH_VOLATILITY_DRAWDOWN`, `MERGER_COMPARABILITY`,
`PROMOTER_OR_GOVERNANCE_RISK`, `SECTOR_METRIC_MISMATCH`.
