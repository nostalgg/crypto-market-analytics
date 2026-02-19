# Phase 1 Domain Validation -- Finance Domain Expert Deliverable

**Agent**: Finance & Market Domain Expert
**Date**: 2026-02-19
**Phase**: 1 -- Foundation
**Status**: COMPLETE

---

## 1. ASSET SELECTION VALIDATION

### Proposed Assets (8 total)

| # | Symbol | CoinGecko API ID | Category | Mainnet Launch | Market Cap Tier |
|---|--------|-------------------|----------|---------------|-----------------|
| 1 | BTC | `bitcoin` | layer1 | 2009-01-03 | Mega-cap |
| 2 | ETH | `ethereum` | layer1 | 2015-07-30 | Mega-cap |
| 3 | SOL | `solana` | layer1 | 2020-03-16 | Large-cap |
| 4 | BNB | `binancecoin` | layer1_exchange | 2017-07-25 | Large-cap |
| 5 | ADA | `cardano` | layer1 | 2017-09-29 | Large-cap |
| 6 | AVAX | `avalanche-2` | layer1 | 2020-09-21 | Mid-cap |
| 7 | LINK | `chainlink` | oracle | 2019-05-30 | Mid-cap |
| 8 | DOT | `polkadot` | layer1_interop | 2020-08-19 | Mid-cap |

> **CRITICAL NOTE**: The CoinGecko API ID for Avalanche is `avalanche-2`, NOT `avalanche`. This is a common pitfall. The Python Expert must use this exact ID.

> **NOTE on DOT date**: Polkadot mainnet launched May 2020 but token transfers were not enabled until August 19, 2020. CoinGecko price data begins at the transfer-enabled date. Use `2020-08-19` as the effective launch date for data purposes.

### Per-Asset Evaluation

**BTC (bitcoin)**
- VERDICT: ESSENTIAL -- must include
- Rationale: The benchmark asset for all crypto analytics. BTC dominance drives market cycles. Required for correlation analysis (Q3), as the reference pair. Every alt-season/risk-off rotation is measured relative to BTC.
- Analytical value: Longest price history, deepest liquidity, cleanest data. Halving events provide natural before/after analytical opportunities.

**ETH (ethereum)**
- VERDICT: ESSENTIAL -- must include
- Rationale: Second-largest asset, different fundamental narrative (smart contracts, DeFi, NFT ecosystem). The Merge (PoS transition, Sep 2022) and Shanghai upgrade (staking withdrawals, Apr 2023) provide protocol-driven event analysis opportunities.
- Analytical value: BTC/ETH correlation is the most-studied pair in crypto. Shows both high correlation in panics and meaningful divergence during ETH-specific events.

**SOL (solana)**
- VERDICT: STRONG INCLUDE
- Rationale: High-beta asset with dramatic boom/bust/recovery cycle. Crashed >95% during FTX (Alameda was a major SOL holder), then recovered from ~$8 to $250+ by late 2024. Multiple network outages create natural anomaly detection targets.
- Analytical value: Extreme drawdown/recovery patterns, volume spikes during outages, decorrelation events. Excellent for volatility regime analysis (Q2) and drawdown analysis (Q4).
- Caveat: Data only available from ~April 2020 on CoinGecko. No data gap issues for our 2022-2025 window.

**BNB (binancecoin)**
- VERDICT: INCLUDE WITH NOTE
- Rationale: Exchange token -- unique category. Its price is partly driven by Binance platform activity, burns, and regulatory news (SEC lawsuit Jun 2023, settlement). Provides diversification from pure Layer 1 narratives.
- Analytical value: Shows exchange-specific risk factors. Less volatile than pure L1 alts, which creates useful contrast in volatility regime analysis. BNB often decouples during Binance-specific news.
- Caveat: Category is ambiguous -- it is both a Layer 1 (BSC/BNB Chain) and an exchange utility token. Classify as `layer1_exchange` to capture both.

**ADA (cardano)**
- VERDICT: INCLUDE -- good for contrast
- Rationale: Large community, but has underperformed most L1s in 2023-2025. Provides the "laggard" archetype needed for cross-asset ranking analysis (Q6). Without underperformers, ranking queries are less interesting.
- Analytical value: Lower volatility than SOL/AVAX, different momentum patterns. Useful for showing that ranking/NTILE queries produce meaningful differentiation.

**AVAX (avalanche-2)**
- VERDICT: INCLUDE
- Rationale: Mid-cap L1 with distinct DeFi ecosystem (Trader Joe, Benqi). Showed notable correlation breaks with BTC during ecosystem-specific events. Provides the "mid-cap L1 alternative" slot.
- Analytical value: Higher beta than BTC/ETH but different pattern than SOL. Good for correlation analysis and volume anomaly detection.
- Caveat: CoinGecko data available from Sep 2020 onward. Full coverage within our recommended date range.

**LINK (chainlink)**
- VERDICT: STRONG INCLUDE -- category diversification
- Rationale: The only non-Layer-1 asset in the selection. Oracle infrastructure token with different fundamental drivers (integration partnerships, CCIP launches) than L1 tokens. This category diversification is analytically valuable.
- Analytical value: LINK often moves on integration news rather than broad market sentiment, creating natural decorrelation opportunities that make Q3 (correlation analysis) more interesting.

**DOT (polkadot)**
- VERDICT: ACCEPTABLE -- weakest inclusion
- Rationale: Interoperability narrative has faded since 2022. Price has underperformed significantly. However, this actually makes it useful as a "declining narrative" archetype alongside ADA.
- Analytical value: Deep drawdown without meaningful recovery (unlike SOL) -- provides a different drawdown profile for Q4. Parachain auction events create identifiable market events.
- Alternative considered: Could swap for MATIC/POL (polygon) or UNI (uniswap) for DeFi exposure. However, DOT provides adequate diversification and its underperformance is itself analytically valuable.

### Asset Selection Summary

**RECOMMENDATION: APPROVE the 8-asset list as proposed.**

The selection provides:
- 2 mega-caps (BTC, ETH) -- benchmark and primary pair
- 3 large-cap L1s (SOL, BNB, ADA) -- varying volatility/beta profiles
- 2 mid-cap diversifiers (AVAX, DOT) -- different narratives
- 1 non-L1 (LINK) -- category diversification

This gives cross-asset ranking (Q6) meaningful spread and correlation analysis (Q3) interesting pair combinations beyond just "everything moves with BTC."

### Category Values for `assets` Table

Use these exact values for the `category` column:

```
BTC  -> 'layer1'
ETH  -> 'layer1'
SOL  -> 'layer1'
BNB  -> 'layer1_exchange'
ADA  -> 'layer1'
AVAX -> 'layer1'
LINK -> 'oracle'
DOT  -> 'layer1_interop'
```

---

## 2. DATE RANGE RECOMMENDATION

### Recommended Range

| Parameter | Value |
|-----------|-------|
| **Start Date** | **2022-01-01** |
| **End Date** | **2025-10-31** |
| **Duration** | 3 years, 10 months |
| **Trading Days** | ~1,400 (crypto trades 365 days/year) |

### Why January 1, 2022?

Starting on 2022-01-01 captures the following analytical arc:

1. **Late bull market** (Jan-Apr 2022): BTC was ~$47K, declining from Nov 2021 ATH ($69K). Captures the distribution/markdown phase transition.
2. **Terra/LUNA collapse** (May 2022): The first major contagion event. Massive correlation spike, volume anomalies, drawdown acceleration.
3. **Cascading failures** (Jun-Jul 2022): 3AC liquidation, Celsius bankruptcy. Extended bear market stress testing.
4. **The Merge** (Sep 2022): ETH-specific protocol event. Tests whether ETH decorrelates from BTC around upgrades.
5. **FTX collapse** (Nov 2022): The defining bear market event. Extreme volatility, correlation to 1, maximum drawdowns for SOL especially.
6. **Bear market bottom** (Dec 2022 - Mar 2023): Accumulation phase. Low volatility regime, volume drying up.
7. **Banking crisis** (Mar 2023): SVB/Signature collapse. Paradoxical BTC rally (safe-haven narrative). USDC depeg.
8. **Recovery and ETF anticipation** (2023-2024): Gradual momentum building, regime shift from low to medium volatility.
9. **BTC Spot ETF approval** (Jan 2024): Institutional capital influx. Volume regime change.
10. **BTC Halving** (Apr 2024): Supply shock event. Historical cycle analysis.
11. **ETH Spot ETF** (Jul 2024): Second ETF milestone.
12. **Post-election rally** (Nov 2024 - Oct 2025): Explosive bull run to $126K ATH.
13. **Trump tariff crash** (Oct 2025): Bear market trigger. Largest liquidation event in crypto history.

### Why NOT start earlier?

- Starting in 2021 would add the euphoric bull run peak (Nov 2021 BTC $69K), but it would also add 12 more months of data ingestion with diminishing analytical return for this project's scope.
- All 8 assets have CoinGecko data available from 2022-01-01.
- 2022-01-01 still captures the full bear-to-bull-to-bear cycle, which is the richest analytical period.

### Why end at October 31, 2025?

- Captures the full cycle from bear through bull to crash.
- The Oct 10, 2025 tariff crash provides a natural "bookend" bear event to pair against the Nov 2022 FTX crash.
- Ending mid-crash (as opposed to mid-recovery) keeps the data narrative clean: the project analyzes a complete market cycle.
- Data will be stable and complete (no partial month issues).

### Alternative: If 2 years is preferred

Use **2023-01-01 to 2025-10-31** (2 years, 10 months). This still captures the bear bottom, full recovery, bull run, and crash. However, you lose the Terra/LUNA and FTX collapses, which are arguably the two most analytically rich events for drawdown and volume anomaly detection. **I strongly recommend the 2022-01-01 start date.**

---

## 3. MARKET EVENTS LIST

### Event Type Categories (see Section 4 for validation)

| event_type | Description |
|------------|-------------|
| `crash` | Market-wide or asset-specific crash (>15% decline in days) |
| `halving` | Bitcoin halving event (supply reduction) |
| `regulatory` | Government/regulatory action affecting crypto |
| `protocol_upgrade` | Major network upgrade or hard fork |
| `market_milestone` | ATH, ETF approval, significant adoption event |
| `exchange_event` | Exchange collapse, hack, or major operational event |
| `macro_event` | External macroeconomic event impacting crypto |

### The 18 Market Events

```
EVENT 01
--------
event_date:       2022-05-09
event_type:       crash
title:            Terra/LUNA collapse -- UST stablecoin depeg triggers death spiral
description:      UST depegged from $1 starting May 7, accelerating on May 9. LUNA collapsed from $80 to near zero. Over $40B in value wiped out, triggering contagion across DeFi and centralized lenders. Massive volume spikes and correlation convergence across all assets.
affected_assets:  ALL

EVENT 02
--------
event_date:       2022-06-13
event_type:       crash
title:            Celsius freezes withdrawals -- crypto lending crisis begins
description:      Celsius Network froze all withdrawals, swaps, and transfers on June 12-13, signaling insolvency. This triggered cascading liquidations across DeFi protocols and amplified bear market selling pressure. Filed Chapter 11 on July 13.
affected_assets:  ALL

EVENT 03
--------
event_date:       2022-06-27
event_type:       exchange_event
title:            Three Arrows Capital ordered to liquidate
description:      BVI court ordered liquidation of 3AC, a $10B crypto hedge fund, after it failed margin calls following Terra/LUNA exposure. The collapse deepened the credit contagion, forcing liquidations of 3AC positions across multiple protocols and exchanges.
affected_assets:  ALL

EVENT 04
--------
event_date:       2022-09-15
event_type:       protocol_upgrade
title:            Ethereum Merge -- transition from Proof of Work to Proof of Stake
description:      Ethereum completed its transition to PoS at block 15537393, reducing energy consumption by ~99.95%. Anticipated for years, the Merge was a sell-the-news event -- ETH declined ~15% in the following week despite successful execution. Provides a clean before/after analytical window for ETH volatility and correlation.
affected_assets:  ETH

EVENT 05
--------
event_date:       2022-11-02
event_type:       exchange_event
title:            CoinDesk exposes Alameda/FTX balance sheet -- FTX crisis begins
description:      CoinDesk published an investigation revealing Alameda Research held $3.66B in FTT tokens as collateral, exposing dangerous financial entanglement with FTX. This report triggered the chain of events leading to FTX collapse. Marks the start of the crisis period.
affected_assets:  ALL,SOL

EVENT 06
--------
event_date:       2022-11-08
event_type:       crash
title:            FTX halts withdrawals -- exchange collapse and $6B bank run
description:      After Binance announced selling $580M in FTT (Nov 6), FTX saw $6B in withdrawals over 72 hours. On Nov 8, FTX froze withdrawals. FTT crashed 80%. Binance offered then withdrew acquisition. FTX filed bankruptcy Nov 11. SOL crashed >50% due to Alameda holdings. Largest single-event drawdown for multiple assets in our dataset.
affected_assets:  ALL,SOL

EVENT 07
--------
event_date:       2023-03-10
event_type:       macro_event
title:            Silicon Valley Bank collapses -- US banking crisis hits crypto
description:      SVB was seized by FDIC on March 10. Circle disclosed $3.3B exposure, causing USDC to depeg to $0.87. After the Fed announced depositor backstop on March 12, BTC rallied 27% in 3 days as safe-haven narrative strengthened. Sharp reversal pattern: crash then rapid recovery.
affected_assets:  ALL

EVENT 08
--------
event_date:       2023-04-12
event_type:       protocol_upgrade
title:            Ethereum Shanghai/Shapella upgrade -- staking withdrawals enabled
description:      Shanghai upgrade activated at 22:27 UTC, enabling validators to withdraw staked ETH for the first time since December 2020. Despite fears of a sell-off, ETH rose 6% to $2,000. Over 4.4M ETH was subsequently deposited into staking, showing net positive demand. Tests sell-the-rumor/buy-the-news pattern.
affected_assets:  ETH

EVENT 09
--------
event_date:       2023-06-05
event_type:       regulatory
title:            SEC sues Binance and Coinbase in consecutive days
description:      SEC filed suit against Binance on June 5 and Coinbase on June 6, 2023, alleging securities law violations. BNB dropped ~10% on the Binance news. Market briefly sold off but recovered within weeks as suits were seen as priced in. BNB shows distinct decorrelation during this event.
affected_assets:  BNB,ALL

EVENT 10
--------
event_date:       2023-10-24
event_type:       market_milestone
title:            BTC spot ETF approval anticipation rally -- BlackRock iShares filing progress
description:      BTC broke above $35,000 for the first time since May 2022, driven by news of BlackRock iShares Bitcoin Trust appearing on DTCC clearing lists. This marked the beginning of the ETF-anticipation rally that dominated Q4 2023. Volume surged well above 30-day averages across major assets.
affected_assets:  BTC

EVENT 11
--------
event_date:       2024-01-10
event_type:       market_milestone
title:            SEC approves 11 spot Bitcoin ETFs -- institutional access begins
description:      SEC approved spot BTC ETFs from BlackRock, Fidelity, and 9 other issuers. Trading began Jan 11. Paradoxically, BTC initially dropped from $49K to $42K over the following 2 weeks (classic sell-the-news), before beginning a sustained rally. Volume spiked 3-4x above 30-day averages.
affected_assets:  BTC,ALL

EVENT 12
--------
event_date:       2024-03-14
event_type:       market_milestone
title:            Bitcoin hits pre-halving ATH of $73,800 -- first ATH before a halving
description:      BTC reached $73,800 on March 14, 2024, setting a new all-time high before the halving for the first time in history. ETF inflows (particularly IBIT) drove institutional buying pressure. This broke the historical pattern where ATHs only occurred 12-18 months post-halving.
affected_assets:  BTC

EVENT 13
--------
event_date:       2024-04-20
event_type:       halving
title:            Fourth Bitcoin halving -- block reward reduced to 3.125 BTC
description:      Block reward halved from 6.25 to 3.125 BTC at block 840,000 on April 19-20, 2024. Unlike previous halvings, BTC had already set an ATH beforehand due to ETF demand. Post-halving price action was muted initially, then began trending higher. Provides comparative analysis opportunity vs. historical halving patterns.
affected_assets:  BTC

EVENT 14
--------
event_date:       2024-07-23
event_type:       market_milestone
title:            Spot Ethereum ETFs begin trading in the US
description:      SEC approved final S-1 filings on July 22; eight spot ETH ETFs began trading July 23 on Nasdaq, NYSE, and CBOE. Initial flows were modest compared to BTC ETFs. ETH showed muted reaction, having already priced in the May 23 initial 19b-4 approval. Useful comparison to BTC ETF launch dynamics.
affected_assets:  ETH

EVENT 15
--------
event_date:       2024-11-06
event_type:       market_milestone
title:            Trump wins 2024 presidential election -- crypto-friendly policy expected
description:      Trump won the presidential election on November 5 (results confirmed Nov 6). BTC surged from $69K on election night to over $75K within hours, then continued rallying. Market priced in expected pro-crypto regulatory environment. Triggered a sustained momentum streak across all crypto assets.
affected_assets:  ALL

EVENT 16
--------
event_date:       2024-12-05
event_type:       market_milestone
title:            Bitcoin crosses $100,000 for the first time
description:      BTC broke the psychologically significant $100K barrier on December 5, 2024, reaching $103,679. Driven by post-election momentum, ETF inflows, and halving supply dynamics. Reached $108,135 by December 17. This milestone attracted mainstream media coverage and new retail participation, visible in volume data.
affected_assets:  BTC,ALL

EVENT 17
--------
event_date:       2025-01-23
event_type:       regulatory
title:            Trump signs crypto executive order -- Strategic Bitcoin Reserve proposed
description:      President Trump signed "Strengthening American Leadership in Digital Financial Technology" executive order. Key provisions: creation of a Strategic Bitcoin Reserve working group, digital asset stockpile, crypto-friendly banking access, and ban on federal CBDC development. SEC also rescinded SAB 121, removing major barrier for institutional custody. Broadly bullish signal.
affected_assets:  ALL

EVENT 18
--------
event_date:       2025-10-10
event_type:       crash
title:            Trump 100% China tariff threat triggers $19B crypto liquidation cascade
description:      Trump announced 100% tariffs on Chinese imports on October 10. BTC fell from $122K to $104K (-15%). SOL crashed 40%. Over $19.1B in leveraged positions liquidated in 24 hours -- the largest single-day liquidation event in crypto history. Altcoins dropped 20-40%. This ended the 2024-2025 bull market and initiated a new drawdown cycle.
affected_assets:  ALL
```

### Events Summary Table (for quick reference)

| # | Date | Type | Short Title | Affected |
|---|------|------|-------------|----------|
| 1 | 2022-05-09 | crash | Terra/LUNA collapse | ALL |
| 2 | 2022-06-13 | crash | Celsius freezes withdrawals | ALL |
| 3 | 2022-06-27 | exchange_event | Three Arrows Capital liquidation | ALL |
| 4 | 2022-09-15 | protocol_upgrade | Ethereum Merge (PoW to PoS) | ETH |
| 5 | 2022-11-02 | exchange_event | Alameda/FTX exposure revealed | ALL,SOL |
| 6 | 2022-11-08 | crash | FTX halts withdrawals | ALL,SOL |
| 7 | 2023-03-10 | macro_event | SVB collapse / banking crisis | ALL |
| 8 | 2023-04-12 | protocol_upgrade | Ethereum Shanghai upgrade | ETH |
| 9 | 2023-06-05 | regulatory | SEC sues Binance and Coinbase | BNB,ALL |
| 10 | 2023-10-24 | market_milestone | BTC ETF anticipation rally begins | BTC |
| 11 | 2024-01-10 | market_milestone | 11 spot BTC ETFs approved | BTC,ALL |
| 12 | 2024-03-14 | market_milestone | BTC pre-halving ATH $73,800 | BTC |
| 13 | 2024-04-20 | halving | Fourth Bitcoin halving | BTC |
| 14 | 2024-07-23 | market_milestone | Spot ETH ETFs begin trading | ETH |
| 15 | 2024-11-06 | market_milestone | Trump wins presidential election | ALL |
| 16 | 2024-12-05 | market_milestone | Bitcoin crosses $100,000 | BTC,ALL |
| 17 | 2025-01-23 | regulatory | Trump crypto executive order signed | ALL |
| 18 | 2025-10-10 | crash | Trump tariff crash / $19B liquidation | ALL |

### Analytical Coverage of Events

The 18 events provide:

- **4 crashes** (Terra, Celsius, FTX, Tariff) -- for drawdown analysis (Q4) and volume anomaly detection (Q5)
- **3 exchange/entity events** (3AC, FTX exposure, FTX collapse) -- for event-driven analysis (Phase 4)
- **2 protocol upgrades** (Merge, Shanghai) -- for ETH-specific before/after analysis
- **2 regulatory events** (SEC suits, Trump EO) -- for regulatory impact on specific assets
- **6 market milestones** (ETF anticipation, BTC ETF, pre-halving ATH, ETH ETF, election, $100K) -- for momentum and volume analysis
- **1 halving** (April 2024) -- for the canonical supply-shock event
- **1 macro event** (SVB crisis) -- for cross-market contagion analysis

Every calendar quarter from Q1 2022 through Q4 2025 has at least one nearby event, ensuring that event-driven queries in Phase 4 will produce results across the entire date range.

---

## 4. EVENT TYPE CATEGORY VALIDATION

### Original Proposed Categories (6)

```
halving, crash, regulatory, protocol_upgrade, market_milestone, exchange_event
```

### Recommendation: ADD ONE CATEGORY

Add `macro_event` as a seventh category.

**Rationale**: The SVB/banking crisis (March 2023) does not fit cleanly into any of the original six categories:
- It is not a `crash` -- crypto actually rallied after the initial shock
- It is not `regulatory` -- it was a banking failure, not a regulatory action
- It is not an `exchange_event` -- SVB was a traditional bank, not a crypto exchange

The `macro_event` category covers external macroeconomic events that impact crypto markets from outside the crypto ecosystem. This distinction matters analytically: macro events often produce different correlation patterns than crypto-native events (e.g., BTC rallied on SVB as a "safe haven" while stocks fell -- the opposite of what happens during crypto-native crashes).

### Renaming Consideration

The original `crash` category could be renamed to `market_crash` for clarity, but `crash` is sufficient and more concise. No other renaming is needed.

### Final Category List (7 types)

| event_type | Definition | Count in Our List |
|------------|------------|-------------------|
| `halving` | Bitcoin block reward halving event | 1 |
| `crash` | Rapid price decline >15% across multiple assets within days | 4 |
| `regulatory` | Government or regulator action directly targeting crypto | 2 |
| `protocol_upgrade` | Major blockchain network upgrade or hard fork | 2 |
| `market_milestone` | Significant adoption event, ATH, or market structure change | 6 |
| `exchange_event` | Exchange or major crypto entity collapse, hack, or crisis | 2 |
| `macro_event` | External macroeconomic event impacting crypto markets | 1 |

### SQL CHECK Constraint

The SQL Expert should implement this as a CHECK constraint on the `market_events` table:

```sql
CONSTRAINT chk_event_type CHECK (
    event_type IN (
        'halving',
        'crash',
        'regulatory',
        'protocol_upgrade',
        'market_milestone',
        'exchange_event',
        'macro_event'
    )
)
```

### `affected_assets` Format Convention

For the `affected_assets` column:
- Use uppercase ticker symbols, comma-separated: `'BTC,ETH,SOL'`
- Use `'ALL'` when the event affected the entire market
- When an event is market-wide but disproportionately affected specific assets, use: `'ALL,SOL'` (meaning all assets were affected, but SOL was affected most)
- This column is `VARCHAR(500)`, not a foreign key -- it is descriptive metadata for query filtering, not a relational join target

---

## 5. DATA QUALITY NOTES FOR PYTHON EXPERT

These notes should be considered during ingestion script development:

1. **CoinGecko API ID for AVAX is `avalanche-2`** -- do not use `avalanche`
2. **DOT price data** begins August 2020 (token transfers enabled). No gap issues for our 2022+ range.
3. **SOL data during outages**: CoinGecko still reports prices during Solana network outages (because SOL trades on centralized exchanges even when the Solana chain is down). Volume data may show anomalies during these periods -- this is expected and analytically interesting, not a data quality issue.
4. **CoinGecko free tier rate limits**: The `/coins/{id}/market_chart/range` endpoint allows daily OHLCV data. Free tier allows 10-30 calls/minute depending on account status. Build in rate limiting and retry logic.
5. **Volume data caveat**: CoinGecko aggregates volume across exchanges. Some exchanges report inflated volume (wash trading). For this project, we accept CoinGecko's aggregated volume as-is and note this limitation in documentation. The volume anomaly detection (Q5) compares each asset's volume to its own moving average, which mitigates cross-asset volume inflation differences.
6. **Weekend patterns**: Despite 24/7 trading, crypto volume is typically 20-40% lower on weekends. The `date_dim.is_weekend` flag enables weekend-aware analysis.

---

## 6. FORMULA DEFINITIONS FOR PHASE 2 (PREVIEW)

These will be fully validated when Phase 2 begins, but are provided here for early planning:

| Metric | Formula | Notes |
|--------|---------|-------|
| `daily_return_pct` | `(close_t - close_{t-1}) / close_{t-1} * 100` | Simple return as percentage. First day per asset is NULL. |
| `vol_7d` | `STDDEV_POP(daily_return_pct) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` | 7-day realized vol (population std dev, not sample). NOT annualized. |
| `vol_30d` | `STDDEV_POP(daily_return_pct) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)` | 30-day realized vol. NOT annualized. |
| `sma_7` | `AVG(close) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` | 7-day simple moving average of closing price. |
| `sma_30` | `AVG(close) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)` | 30-day simple moving average of closing price. |
| `volume_ratio_30d` | `volume / AVG(volume) OVER (ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)` | Today's volume divided by prior 30-day average. Values >2.0 flag anomalies. Excludes current day from average denominator. |

> **Annualization note**: When annualizing volatility for display purposes (not stored in `daily_metrics`), use `vol_30d * SQRT(365)`. Crypto trades every day, so the annualization factor is sqrt(365), NOT sqrt(252).

---

## SIGN-OFF

This deliverable validates the domain requirements for Phase 1. The SQL Expert may proceed with DDL (CREATE TABLE statements using the asset list, category values, date range, and event type CHECK constraint defined above). The Python Expert may proceed with the ingestion script using the CoinGecko API IDs, date range, and data quality notes provided.

**Next actions**:
- SQL Expert: Create schema DDL incorporating the 7 event types as a CHECK constraint
- Python Expert: Build ingestion script using the 8 CoinGecko API IDs and 2022-01-01 to 2025-10-31 date range
- Python Expert: Create `populate_events.py` script using the 18 events listed above
- Finance Expert (Phase 2): Full formula validation for daily_metrics computation
