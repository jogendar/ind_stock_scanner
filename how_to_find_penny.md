# How to Find Potential Penny-Stock Multibaggers

## Purpose

This note records what the four multibaggers in the V2 stable-top-50
backtest can teach us about finding future candidates without using hindsight.
It is research guidance, not an investment recommendation.

The portfolio rule bought a stock once it had remained in the daily V2 top 50
for 20 consecutive NSE trading snapshots. Prices below Rs 20 were treated as
"penny stocks." That price-only definition is useful for screening but is not
enough to establish that a company is genuinely small or cheap.

The four observed multibaggers, valued through 30 September 2026, were:

| Stock | Simulated purchase | Entry price | Confirmation rank | Later multiple |
|---|---:|---:|---:|---:|
| ONELIFECAP | 13 Nov 2025 | Rs 13.68 | 1 | 2.85x |
| SANGINITA | 13 Nov 2025 | Rs 11.28 | 6 | 6.17x |
| VIJIFIN | 26 Feb 2026 | Rs 2.09 | 27 | 7.55x |
| AHCL | 29 Jun 2026 | Rs 16.14 | 11 | 2.14x |

## Main finding

There was no single pattern shared by all four winners:

| Stock | Quality of signal available by purchase date | Dominant explanation |
|---|---|---|
| AHCL | Strong | Sustained earnings growth plus price/volume confirmation |
| VIJIFIN | Moderate | Quarterly turnaround, followed by a large capital raise |
| SANGINITA | Weak before purchase; stronger later | Change of control and acquisition announced months later |
| ONELIFECAP | Weak and high-risk | Later rights issue and speculative rerating despite poor fundamentals and regulatory risk |

AHCL and, to a lesser extent, VIJIFIN support adding better fundamental-change
features. SANGINITA supports a separate corporate-event scanner. ONELIFECAP is
a warning that a stock can become a multibagger even when a prudent model
should reject it.

## Case studies

### AHCL: a fundamentally detectable winner

The strongest public evidence existed before the simulated purchase:

- FY26 revenue was approximately Rs 176.5 crore, up from Rs 120.3 crore.
- FY26 profit after tax was approximately Rs 27.8 crore, up from Rs 20.5 crore.
- The entry snapshot showed a net margin near 16.6%, interest coverage around
  12.4x, a current ratio around 2.87, ROE near 17%, and relatively low debt.
- Its V2 market-confirmation pillar was 25/25.
- The FY26 results were published on 29 May 2026, immediately before the stock
  began its 20-session stable period on 1 June.
- The pre-IPO history also showed profit increasing from roughly Rs 5.8 crore
  in FY23 to Rs 9.7 crore in FY24 and Rs 20.5 crore in FY25.

This is the cleanest example of a potentially repeatable pattern: multi-year
profit growth, a fresh earnings acceleration, healthy margins and balance-sheet
ratios, and strong market confirmation appearing together.

AHCL also exposes a flaw in the price-only penny-stock definition. A five-for-one
stock split and a 1:1 bonus became effective on 24 April 2026. Together they
turned one old share into ten shares and mechanically moved the nominal price
below Rs 20. At entry, the company already had a market capitalisation of about
Rs 858 crore. It was therefore a low-priced stock, but not a classic tiny penny
stock.

Sources:

- [NSE AHCL announcement history](https://www.nseindia.com/companies-listing/corporate-filings-announcements?symbol=AHCL)
- [FY26 results press-release filing](https://nsearchives.nseindia.com/corporate/ANLON2013_30052026113136_INTIMATION.pdf)
- [Pre-IPO prospectus](https://nsearchives.nseindia.com/corporate/FP_INE0Y8W01017_01SEP2025.pdf)
- [NSE quote and corporate-action record](https://www.nseindia.com/get-quotes/equity?symbol=AHCL)

### VIJIFIN: a turnaround plus a later capital event

The useful early signal was the result filed on 16 January 2026:

- Q3 interest income was approximately Rs 1.59 crore.
- Q3 profit after tax was approximately Rs 0.94 crore.
- Nine-month profit after tax was only about Rs 0.52 crore.

The last two figures imply that the first six months collectively lost roughly
Rs 0.43 crore before the Q3 profit reversal. A quarterly turnaround detector
could have identified this change before the 26 February purchase. Static
five-year growth rates did not represent it well.

The major public catalyst came after purchase. On 24 March, the company proposed
up to 12.75 crore convertible warrants for aggregate consideration of as much
as Rs 35.7 crore. That proposed raise was larger than the company's approximate
Rs 30.5 crore market capitalisation at entry. In June, 8.85 crore warrants were
allotted.

The capital injection could transform a very small NBFC, but it also creates
substantial dilution. A scanner should score the possible operating benefit and
the dilution separately instead of treating every fund raise as positive.

Sources:

- [16 January 2026 NSE financial filing](https://nsearchives.nseindia.com/corporate/ixbrl/INTEGRATED_FILING_NBFC_INDAS_135428_16012026175414_iXBRL_WEB.html)
- [March warrant proposal](https://nsearchives.nseindia.com/corporate/VIJIFIN_01042026163246_NoticeEGM.pdf)
- [June warrant allotment](https://nsearchives.nseindia.com/corporate/VIJIFIN_16062026184102_OUTCOME.pdf)

### SANGINITA: a later takeover rather than an early fundamental signal

The operating picture around the 13 November 2025 purchase was weak:

- September-quarter sales had declined materially.
- The company reported a quarterly loss.
- ROE was negative.
- Promoter holding in the local data had declined by about 7.2 percentage
  points.

The positive observations were a current ratio near 2, positive free cash flow,
a low price-to-book ratio, a very small market capitalisation, and a strong
19/25 market-confirmation score. These characteristics described a distressed
value and accumulation candidate, not an obvious future 6x business.

The transformative information became public in March 2026, approximately four
months after purchase. B N G Investment and Anubhav Agarwal entered into a
change-of-control transaction involving promoter-share purchases, a preferential
issue of 3.44 crore shares, about Rs 25 crore of cash investment, and the
acquisition of Agastya Green Energy through a share swap. A mandatory open offer
was priced at Rs 13.55.

The archived price was around Rs 19 by 1 April and later reached about Rs 69.57.
Therefore, an event scanner acting only after public disclosure could still
have captured a substantial portion of the move. There is no sound basis for
claiming that the takeover was predictable from the November fundamentals.
Early price/volume accumulation may have been informative, but it cannot be
treated as evidence of advance knowledge without a much wider control study.

Sources:

- [SEBI takeover filing page](https://www.sebi.gov.in/filings/takeovers/mar-2026/sanginita-chemicals-limited_100567.html)
- [SEBI letter of offer](https://www.sebi.gov.in/sebi_data/commondocs/jun-2026/Sanginita%20Chemicals%20Limited%20-%20LOF_p.pdf)
- [Company investor-relations page](https://www.sanginitachemicals.co.in/investor-relation.html)

### ONELIFECAP: a multibagger that a prudent model should probably reject

The V2 score was 70 at purchase, but the underlying operating data included:

- Negative EPS and ROE.
- Negative operating and net margins.
- Negative interest coverage.
- A trailing loss of approximately Rs 5.1 crore.
- A September 2025 quarterly loss despite improved revenue.

The fundamental-growth pillar incorrectly received 20/20. Percentage-growth
formulae can produce apparently high values when the starting or ending profit
is negative or close to zero. Growth points should never be awarded from such
mathematically misleading bases.

The company authorized a rights issue on 10 December 2025, after the simulated
purchase. It offered up to 2.4 crore new shares versus 1.336 crore existing
shares, in a ratio of 300 new shares for every 167 held, at Rs 15 each. The
maximum Rs 36 crore raise was mainly intended to fund margin requirements in a
subsidiary. This was large enough to change the business but also highly
dilutive.

More importantly, material regulatory warnings were public before purchase.
SEBI's March 2025 order restrained the company and certain management from
accessing the securities market for one year and imposed penalties. A recovery
notice followed in August 2025. These facts should be a hard exclusion or a
very large governance penalty even though the share price subsequently rose.

The portfolio calculation also tracks only the original shares. It does not add
proceeds from selling the rights entitlement or the additional capital and
shares that would result from exercising it. ONELIFECAP's economic return
therefore requires separate corporate-action accounting.

Sources:

- [Official rights-issue letter of offer](https://nsearchives.nseindia.com/corporate/ONELIFECAP_12022026184521_LOF_Onelife_12022026_revised_with_Cover_sd.pdf)
- [SEBI March 2025 final order](https://www.sebi.gov.in/sebi_data/commondocs/mar-2025/onelife_capital_advisors_limited_p.pdf)
- [SEBI August 2025 recovery notice](https://www.sebi.gov.in/enforcement/recovery-proceedings/aug-2025/notice-of-demand-dated-01-08-2025-issued-under-rc-no-8845-2025-drawn-against-onelife-capital-advisors-limited-pan-aaaco9540l-in-the-matter-of-onelife-capital-advisors-limited_95771.html)

## Candidate signals worth testing

### 1. Quarterly operating inflection

Prefer changes that could have been calculated immediately after an exchange
filing:

- Loss to profit in the latest quarter.
- Revenue acceleration versus both the preceding quarter and the same quarter
  last year.
- Expanding operating and net margins.
- Interest coverage crossing above 2x or 3x.
- Operating cash flow becoming positive and broadly matching profit.
- Falling debt while revenue and profit rise.
- Turnaround sustained for two quarters rather than a one-quarter accounting
  gain.

This feature family is most relevant to AHCL and VIJIFIN.

### 2. Corporate-event detection

Parse point-in-time NSE, BSE and SEBI filings for:

- Change of control and open offers.
- Preferential issues and convertible warrants.
- Rights issues.
- Acquisitions and share swaps.
- New capacity, regulatory approvals, large orders, or entry into a materially
  different business.
- Fresh investor presentations or earnings calls following important results.

An announcement alone is not a buy signal. Score its economic importance using:

- Cash entering the company divided by pre-announcement market capitalisation.
- Increase in fully diluted share count.
- Price paid by the new investor versus the undisturbed market price.
- Whether proceeds fund productive capacity, working capital, debt repayment,
  a subsidiary, or only general corporate purposes.
- Track record and financial strength of the acquirer or allottee.
- Evidence that revenue or profit can materially increase.

This feature family is most relevant to SANGINITA and VIJIFIN. ONELIFECAP shows
why fund raising without quality and governance checks is unsafe.

### 3. Market confirmation after a fundamental or event signal

Useful confirmation features include:

- 20-day volume compared with 120-day volume.
- A rising 20-day moving average above the 100-day average.
- Positive but not already extreme three-month momentum.
- Price close to a 52-week high.
- Adequate traded value so that the result is not caused by a few tiny trades.
- Persistence for several sessions after the filing rather than a one-day
  upper-circuit reaction.

The order matters: identify a fundamental change or public event first, then
use price and volume as confirmation. Pure momentum in illiquid penny stocks is
too easy to manipulate.

## Required risk controls

The scanner should penalize or exclude:

- SEBI debarments, enforcement orders and recovery proceedings.
- Qualified audit opinions, going-concern warnings and repeated filing delays.
- Negative current PAT, EPS, ROE or interest coverage when a growth formula
  otherwise appears positive.
- Large unexplained related-party transactions.
- Promoter holding declines or heavy promoter pledging.
- Preferential or warrant dilution above a chosen threshold, such as 25%,
  unless independently justified by expected operating gains.
- A one-month price increase above 100% without a proportionate public business
  development.
- Extremely low traded value or repeated upper circuits with little volume.
- Dependence on exceptional or other income instead of operating profit.

These controls will deliberately reject some future winners. That is acceptable:
the objective should be to find repeatable, investable multibaggers, not to
explain every stock that happens to rise.

### Safeguards implemented in the current V2

The HARDWYN and INDOWIND review led to a deliberately small update rather than
a wholesale reweighting of the model:

- Deduct two points each for negative free cash flow, ROE/ROCE below 5%, P/E
  above 50, EV/EBITDA above 30, and simultaneous six- and twelve-month declines
  worse than 25%.
- Cap the score at 49 when negative free cash flow, sub-5% capital returns, and
  extreme valuation occur together. A single weak metric cannot trigger this
  gate.
- Exclude an extremely valued stock from ranked candidate lists when both free
  cash flow and ROE/ROCE are missing, because neither core risk check can be
  performed. Other missing values continue to earn zero points without an
  automatic penalty.
- Deduct ten points for a known qualified audit and two or four points for
  dilution of at least 10% or 25%, when point-in-time filing data is supplied.
- Remove volume-accumulation points when volume is known to coincide with a
  corporate-action window.

Stronger reweighting was rejected after testing. It reduced archived V2 AUC
from 0.598 to 0.502 and selected no historical winners at the default cutoff.
The smaller overlay retained the original growth and rebound signals: on 386
resolved first entries, AUC was 0.599, average precision was 0.100, and six of
the 17 winners remained in the 52 eligible stocks scoring at least 50. These
figures use the same limited archive and are regression evidence, not an
independent out-of-sample validation.

The fixed 20-session stable-top-50 simulation returned -0.55%, versus +4.93%
for the prior V2. The entire headline deterioration came from rejecting
SANGINITA, whose takeover-driven 6x outcome was announced months after entry
and was not supported by a strong entry signal. Excluding SANGINITA from the
old result produces -0.89%, compared with -0.55% for the fixed model. Retaining
that one stock merely because its unknowable catalyst later occurred would be
hindsight overfitting.

## Fix the penny-stock definition

Do not rely on nominal price alone. A better universe definition should combine:

- Price below Rs 20.
- A market-cap band, for example below Rs 500 crore or Rs 1,000 crore.
- Minimum average daily traded value.
- A flag for recent splits and bonus issues.
- Fully diluted market capitalisation after announced rights, warrants and
  preferential issues.

Recently split stocks such as AHCL can remain in a separate low-priced-growth
universe, but they should not be treated as equivalent to a Rs 20-30 crore
microcap.

## Concrete V3 scoring changes to test

1. Gate profit, EPS and margin growth points on positive current values. Do not
   reward growth across a negative base.
2. Add a quarterly-inflection pillar based only on filings available by the
   scoring date.
3. Add a filing-event pillar with separate positive impact and dilution scores.
4. Add hard governance vetoes or a penalty large enough to remove a candidate
   such as ONELIFECAP.
5. Calculate market cap and valuation on a fully diluted basis where announced
   securities are likely to be issued.
6. Detect recent splits and bonuses so a mechanical price reduction does not
   create false penny-stock status.
7. Require post-event price/volume persistence, but cap momentum points to
   avoid selecting only already-pumped shares.
8. Preserve the reason for every score in the output, including filing date,
   source URL, event age and dilution estimate.

## How to validate without overfitting

Studying only the four winners creates selection and hindsight bias. Every
plausible winner has an attractive story after the fact. New rules must be
tested on the complete universe:

1. Timestamp every financial result and corporate filing by its public
   dissemination time.
2. Recalculate scores using only data that was public on each historical date.
3. Apply event rules to every company with that event, not just these winners.
4. Keep event discovery and outcome measurement separate.
5. Use walk-forward periods: design on earlier months and evaluate unchanged
   rules on later months.
6. Compare each event candidate with similar market-cap, sector, liquidity and
   momentum controls.
7. Report precision, recall, top-N hit rate, median return, drawdown and
   portfolio return rather than only the best winners.
8. Include delisted and unresolved stocks at zero after checking ticker changes,
   so survivorship bias cannot improve the results.
9. Model splits, bonuses, rights entitlements, mergers and additional capital
   contributions explicitly.
10. Prefer a small number of economically defensible rules over repeated
    tuning against these four outcomes.

## Practical conclusion

The most promising repeatable combination from this exploration is:

> positive quarterly operating inflection + adequate financial survival + a
> material public catalyst + persistent price/volume confirmation + no major
> governance warning.

AHCL largely fit this pattern. VIJIFIN partially fit it, with much higher
dilution and credit risk. SANGINITA became interesting only after the public
change-of-control filing. ONELIFECAP rose, but its financial and regulatory
record makes it an example of a winner that a robust process should be willing
to miss.
