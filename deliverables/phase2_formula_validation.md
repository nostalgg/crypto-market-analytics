# Phase 2 Formula Validation -- Finance Domain Expert Deliverable

**Agent**: Finance & Market Domain Expert
**Date**: 2026-02-19
**Phase**: 2 -- Metrics Computation
**Status**: COMPLETE
**Data Source**: Yahoo Finance (OHLCV -- all fields populated)

---

## 0. PRELIMINARY DECISIONS

Before defining formulas, four design decisions must be resolved. Each decision is justified below and carries forward into all formula specifications.

### Decision 1: Storage Format for `daily_return_pct`

**DECISION: Store as percentage (e.g., 5.0 for a 5% gain), NOT as decimal fraction (0.05).**

Justification:
- The schema defines `daily_return_pct` as `NUMERIC(10,6)`. A percentage representation like `5.123456` fits comfortably. A decimal representation like `0.051234` would waste four digits of the integer portion and could cause confusion about whether a value has been multiplied by 100 yet.
- The column name itself contains `_pct`, which conventionally means "already in percentage form."
- Phase 1 preview formula (Section 6) explicitly shows `* 100`, confirming the percentage convention.
- When computing volatility (STDDEV of returns), using percentage-form returns yields volatility in percentage points, which is the standard unit in financial reporting (e.g., "BTC 30-day vol is 3.2%").
- If anyone downstream needs the decimal fraction, dividing by 100 is trivial.

**Convention**: A +5% daily return is stored as `5.000000`. A -2.3% daily return is stored as `-2.300000`.

### Decision 2: STDDEV_POP vs STDDEV_SAMP for Rolling Volatility

**DECISION: Use STDDEV_POP (population standard deviation).**

Justification:
- The rolling window IS the population of interest. When we compute "7-day volatility," we mean the realized volatility over exactly those 7 days -- not an estimate of some larger population's volatility inferred from a 7-day sample.
- STDDEV_SAMP divides by (N-1), producing a Bessel-corrected estimate of a population parameter from a sample. That is the wrong model here. We are not sampling from a larger set; we are measuring the full dispersion of returns within the defined window.
- For N=7, the difference is material: STDDEV_SAMP divides by 6 instead of 7, inflating the result by ~8%. For N=30, the difference shrinks to ~1.7%, but using POP is still conceptually correct.
- Industry convention for realized/historical volatility in quantitative finance uses the population formula. Bloomberg's HVOL function, for example, uses N in the denominator.
- Phase 1 preview (Section 6) already specified STDDEV_POP.

### Decision 3: `volume_ratio_30d` -- Include or Exclude Current Day from Denominator

**DECISION: EXCLUDE the current day from the 30-day average denominator.**

Justification:
- The purpose of volume_ratio_30d is to detect whether TODAY's volume is anomalous relative to RECENT HISTORY. Including today's volume in the average contaminates the baseline: a massive volume spike would inflate its own reference average, dampening the very signal we want to detect.
- The formula uses `ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING`, which gives us the prior 30 calendar days, excluding the current row. Today's volume is then divided by this historical average.
- This is the standard approach for volume anomaly detection. Think of it as: "Is today's volume unusual compared to the recent past?" The "recent past" must not include today.
- Phase 1 preview (Section 6) already specified this exclusion.

### Decision 4: Minimum Observations for Rolling Metrics

**DECISION: Enforce minimum observation thresholds. Metrics are NULL when the window is incomplete.**

| Metric | Window Size | Minimum Observations Required | NULL Rule |
|--------|-------------|-------------------------------|-----------|
| `vol_7d` | 7 days | 7 (requires 7 return values = 8 price observations) | NULL for the first 7 trading days per asset (day 1 has no return; days 2-7 have only 1-6 returns) |
| `vol_30d` | 30 days | 30 (requires 30 return values = 31 price observations) | NULL for the first 30 trading days per asset |
| `sma_7` | 7 days | 7 price observations | NULL for the first 6 trading days per asset |
| `sma_30` | 30 days | 30 price observations | NULL for the first 29 trading days per asset |
| `volume_ratio_30d` | 30 prior days | 30 prior volume observations | NULL for the first 30 trading days per asset |

Justification:
- Computing a "7-day volatility" from 3 data points is misleading. The resulting value is unstable and not comparable to proper 7-day windows later in the series.
- NULLs are strongly preferred over partial-window values. Analysts can filter on `vol_7d IS NOT NULL` to get only fully-formed windows.
- For a 2022-01-01 start date, the first valid `vol_30d` value appears on approximately 2022-02-01 (31st price observation = 30th return). This is acceptable; we do not lose meaningful analytical range.

**Implementation note**: PostgreSQL window functions with `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` will happily compute over partial windows (e.g., on day 3, the window contains only 3 rows). The Python or SQL computation layer MUST explicitly set the metric to NULL when the window is not fully populated. Use a CASE expression:
```sql
CASE WHEN COUNT(*) OVER (PARTITION BY asset_id ORDER BY date
       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 7
     THEN STDDEV_POP(daily_return_pct) OVER (...)
     ELSE NULL
END
```

---

## 1. METRIC: `daily_return_pct`

### 1.1 Definition

The simple daily percentage return, measuring the close-to-close price change as a percentage.

### 1.2 Formula

**Mathematical notation:**

```
R_t = ((P_t - P_{t-1}) / P_{t-1}) * 100
```

Where:
- `R_t` = daily return on day t, in percentage points
- `P_t` = closing price on day t
- `P_{t-1}` = closing price on the previous calendar day for the same asset

**SQL pseudocode:**

```sql
daily_return_pct =
    (close - LAG(close, 1) OVER (PARTITION BY asset_id ORDER BY date))
    / LAG(close, 1) OVER (PARTITION BY asset_id ORDER BY date)
    * 100
```

**Python pseudocode:**

```python
daily_return_pct = df.groupby('asset_id')['close'].pct_change() * 100
```

### 1.3 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| First day of data for an asset | NULL | No previous close exists. LAG returns NULL; any arithmetic with NULL yields NULL. |
| Previous close is zero | IMPOSSIBLE | The schema enforces `CHECK (close > 0)`. Division by zero cannot occur. |
| Close equals previous close | 0.000000 | A zero return is valid and meaningful (flat day). |
| Extreme move (close 2x previous) | 100.000000 | Valid. During FTX collapse, SOL moved >40% in a single day. |
| Missing day (gap in data) | Not applicable | Yahoo Finance provides data for all calendar days. If a gap exists, the LAG function skips to the previous available row for that asset, which is the correct behavior (the return spans the gap period). Alternatively, the ingestion script can insert rows for missing dates and carry forward the previous close, making the return 0%. **Preferred approach: do NOT fill gaps. Let LAG span the gap. Document that the return covers >1 calendar day in that case.** |

### 1.4 Common Mistakes

1. **Forgetting PARTITION BY asset_id**: Without it, LAG pulls the previous row regardless of which asset it belongs to. The first row of each asset (ordered by date) would compute a return against a different asset's price.
2. **Using log returns instead of simple returns**: Log returns (`LN(P_t / P_{t-1})`) are appropriate for multi-period compounding analysis but NOT for single-day return reporting. Simple returns are more intuitive and match what users expect when reading "daily return." Use simple returns unless there is a specific analytical reason to switch.
3. **Storing as decimal instead of percentage**: See Decision 1. The column name ends in `_pct`; store as percentage.
4. **Not multiplying by 100**: Forgetting the `* 100` yields a decimal fraction in a column named `_pct`. This will cause all downstream consumers (volatility, rankings) to produce values 100x too small.
5. **Using open-to-close instead of close-to-close**: Open-to-close measures intraday return. Close-to-close measures total daily return including overnight gaps. The standard convention for "daily return" in financial analysis is close-to-close.

### 1.5 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | -100 < daily_return_pct < +500 for crypto | A value of -100 means the asset went to zero (should never happen for our 8 assets). Values beyond +500% in a single day are implausible. |
| Typical values | -15% to +15% for most days | More than 5% of values outside this range per asset suggests a data issue. |
| NULL count | Exactly 1 NULL per asset (first day) | If more NULLs exist, there may be data gaps or a computation bug. |
| Mean over full period | Between -0.5% and +0.5% daily | A mean daily return of +5% would imply 6000% annual return -- clearly wrong. |
| Cross-check | SUM of daily returns over any window should approximately match the actual price change (with compounding error) | Large divergence indicates a formula bug. |

---

## 2. METRIC: `daily_range_pct`

### 2.1 Definition

The intraday price range expressed as a percentage of the low price. Measures the maximum price swing within a single trading day. This is a measure of intraday volatility, complementing the close-to-close `daily_return_pct`.

### 2.2 Formula

**Mathematical notation:**

```
Range_t = ((H_t - L_t) / L_t) * 100
```

Where:
- `H_t` = high price on day t
- `L_t` = low price on day t

**SQL pseudocode:**

```sql
daily_range_pct = (high - low) / low * 100
```

**Python pseudocode:**

```python
daily_range_pct = (df['high'] - df['low']) / df['low'] * 100
```

### 2.3 Why Divide by Low (Not Close or Open)?

The denominator choice matters for interpretability:
- **Dividing by low** gives the "maximum possible gain from intraday low to intraday high" -- the most intuitive range measure. It also guarantees a non-negative result (since high >= low).
- Dividing by close is less intuitive (close may be anywhere within the day's range).
- Dividing by open produces "intraday range relative to starting price," which is also valid but less common.
- Dividing by the midpoint `(high + low) / 2` is sometimes used in the Parkinson estimator but adds complexity without clear benefit here.

The low-denominator convention is used because:
1. `high >= low` always, so the result is always >= 0 (no sign ambiguity).
2. It directly answers "how wide was the trading range today, as a percentage?"

### 2.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| High or low is NULL | NULL | If Yahoo Finance fails to provide OHLC data for a day, the range cannot be computed. |
| High equals low | 0.000000 | The asset did not move intraday. This can happen on very low-liquidity days or during exchange maintenance. A zero range is valid. |
| Low is zero | IMPOSSIBLE | A zero low price would imply the asset traded at zero, which cannot happen for our 8 assets. Additionally, close > 0 is enforced. If low were somehow zero, this would be a data error. |
| High < low | DATA ERROR | This should never happen. If detected, flag as a data quality issue. Consider adding a CHECK constraint or validation step: `CHECK (high >= low)`. |
| First day of data for an asset | COMPUTABLE | Unlike daily_return_pct, daily_range_pct does NOT require a previous day. It only uses the current day's high and low. The first day should have a value (not NULL), assuming OHLCV data is present. |

### 2.5 Common Mistakes

1. **Using (high - low) / close**: This produces a different value and can exceed 100% for a day where the close is near the low and the range was large. It is also less standard for range-as-percentage calculations.
2. **Confusing daily_range_pct with daily_return_pct**: Range is always non-negative (it measures spread, not direction). Return can be positive or negative.
3. **Not handling NULL high/low**: With Yahoo Finance, OHLC should always be populated for trading days. But defensive NULL handling is still good practice.
4. **Applying this to data where high/low are interpolated**: If a data source only provides close prices and fills high=low=close, the range will always be zero. With Yahoo Finance OHLCV this is not an issue -- real high and low values are provided.

### 2.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | 0 to ~60% for crypto | Zero is valid (flat day). Values above 60% are extremely rare even in crypto. |
| Typical values | 1% to 10% for BTC; 2% to 20% for altcoins | If most days are 0%, the high/low data is likely wrong. |
| Always non-negative | daily_range_pct >= 0 always | Any negative value indicates high < low -- a data error. |
| Correlation with abs(daily_return_pct) | Moderate positive correlation | If range and absolute return are uncorrelated, something is wrong with one of the metrics. |
| Spikes during known crash dates | daily_range_pct should spike on FTX collapse, Terra/LUNA, tariff crash dates | If it does not spike on known volatility events, the data is suspect. |

---

## 3. METRIC: `vol_7d`

### 3.1 Definition

The 7-day rolling realized volatility, computed as the population standard deviation of the 7 most recent daily_return_pct values. NOT annualized. Expressed in percentage-point units (because daily_return_pct is in percentage form).

### 3.2 Formula

**Mathematical notation:**

```
vol_7d_t = sqrt( (1/7) * SUM_{i=t-6}^{t} (R_i - R_bar)^2 )
```

Where:
- `R_i` = daily_return_pct on day i
- `R_bar` = mean of R_i over the 7-day window = (1/7) * SUM_{i=t-6}^{t} R_i

Equivalently (population standard deviation):

```
vol_7d_t = STDDEV_POP( R_{t-6}, R_{t-5}, ..., R_t )
```

**SQL pseudocode:**

```sql
vol_7d = CASE
    WHEN COUNT(daily_return_pct) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) = 7
    THEN STDDEV_POP(daily_return_pct) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
    ELSE NULL
END
```

**Python pseudocode:**

```python
# Using pandas with ddof=0 for population std dev
vol_7d = (
    df.groupby('asset_id')['daily_return_pct']
    .rolling(window=7, min_periods=7)
    .std(ddof=0)
    .reset_index(level=0, drop=True)
)
```

### 3.3 Window Frame Specification

```sql
PARTITION BY asset_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
```

- **PARTITION BY asset_id**: Each asset has independent volatility. Never mix returns across assets.
- **ORDER BY date**: Chronological order is essential for time-series windows.
- **ROWS BETWEEN 6 PRECEDING AND CURRENT ROW**: Includes the current row and the 6 rows before it = 7 rows total. This is a ROWS frame (physical row count), NOT a RANGE frame (logical date range). If there are data gaps, ROWS will still grab exactly 7 consecutive rows, which may span more than 7 calendar days. This is acceptable because our data is expected to be gap-free (daily data from Yahoo Finance).
- **Why ROWS, not RANGE?**: ROWS guarantees exactly 7 data points (when the window is full). RANGE BETWEEN INTERVAL '6 days' would include all rows within a calendar range, which could be fewer than 7 if dates are missing or more than 7 if multiple rows exist per date (should not happen due to UNIQUE constraint). ROWS is more predictable and reliable.

### 3.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| Fewer than 7 returns available | NULL | The CASE expression returns NULL when COUNT < 7. The first valid vol_7d for each asset appears on the 8th price observation (7th return). |
| All 7 returns are identical | 0.000000 | Mathematically valid. Zero std dev means no dispersion. Extremely unlikely in practice but not a bug. |
| One or more returns in the window are NULL | Depends on SQL behavior | STDDEV_POP in PostgreSQL ignores NULLs and computes over the non-NULL values. The COUNT check should count only non-NULL values: use `COUNT(daily_return_pct)` (not `COUNT(*)`). This ensures we require 7 actual return values, not just 7 rows. |
| An extreme outlier return in the window | Large vol_7d | This is expected and desirable. A single +40% day (like SOL post-FTX recovery) in a 7-day window should produce extreme vol_7d. This is not a bug; it is the metric working correctly. |

### 3.5 Common Mistakes

1. **Using STDDEV_SAMP instead of STDDEV_POP**: See Decision 2. STDDEV_SAMP inflates the result by sqrt(7/6) = ~8.2% for a 7-day window. This is both conceptually wrong and numerically distorting.
2. **Computing STDDEV of prices instead of returns**: Standard deviation of price levels is meaningless for volatility. Prices are non-stationary. Volatility is defined over returns.
3. **Annualizing when the column should not be annualized**: vol_7d and vol_30d in daily_metrics are stored as daily-frequency, NOT annualized. If a downstream query needs annualized volatility, it applies `* SQRT(365)` at query time.
4. **Using ROWS BETWEEN 7 PRECEDING AND CURRENT ROW** (off by one): This creates an 8-row window. The correct frame for 7 rows is `6 PRECEDING AND CURRENT ROW`.
5. **Forgetting PARTITION BY asset_id**: The window would span across different assets, mixing BTC and ETH returns in the same STDDEV computation.
6. **Not enforcing minimum observations**: Allowing partial-window STDDEV computation produces unreliable early values.
7. **Using pandas `.std()` without `ddof=0`**: Pandas defaults to `ddof=1` (sample std dev). You must explicitly pass `ddof=0` for population std dev.

### 3.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | 0 to ~30 percentage points | vol_7d > 30 means the average daily move over a week was enormous. Plausible during extreme events (FTX crash week) but should be very rare. |
| Typical values | 0.5 to 5.0 for BTC; 1.0 to 10.0 for altcoins | Values consistently near zero suggest returns are being computed wrong (e.g., not multiplied by 100). |
| NULL count per asset | Exactly 7 NULLs (first day has no return + 6 more days with fewer than 7 returns) | If more or fewer NULLs exist, the minimum-observation logic is wrong. |
| Spikes around known events | vol_7d should peak in the week following FTX crash (Nov 8-15, 2022), Terra/LUNA (May 9-16, 2022), tariff crash (Oct 10-17, 2025) | Absence of spikes means returns or the window is wrong. |
| Relationship to vol_30d | vol_7d is noisier (higher variance) than vol_30d, but their long-run means should be similar | If vol_7d is systematically 3x larger than vol_30d, there is likely a window-size or formula error. |

---

## 4. METRIC: `vol_30d`

### 4.1 Definition

The 30-day rolling realized volatility, computed as the population standard deviation of the 30 most recent daily_return_pct values. NOT annualized. Expressed in percentage-point units.

### 4.2 Formula

**Mathematical notation:**

```
vol_30d_t = sqrt( (1/30) * SUM_{i=t-29}^{t} (R_i - R_bar)^2 )
```

Where:
- `R_i` = daily_return_pct on day i
- `R_bar` = mean of R_i over the 30-day window

**SQL pseudocode:**

```sql
vol_30d = CASE
    WHEN COUNT(daily_return_pct) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) = 30
    THEN STDDEV_POP(daily_return_pct) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
    ELSE NULL
END
```

**Python pseudocode:**

```python
vol_30d = (
    df.groupby('asset_id')['daily_return_pct']
    .rolling(window=30, min_periods=30)
    .std(ddof=0)
    .reset_index(level=0, drop=True)
)
```

### 4.3 Window Frame Specification

```sql
PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
```

- 29 PRECEDING + CURRENT ROW = 30 rows total.
- Same reasoning as vol_7d for ROWS vs RANGE: ROWS guarantees exactly 30 data points.

### 4.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| Fewer than 30 returns available | NULL | First valid vol_30d appears on the 31st price observation (30th return). |
| All 30 returns identical | 0.000000 | Valid but astronomically unlikely. |
| Mix of NULLs in the window | NULL if fewer than 30 non-NULL returns | Same defensive logic as vol_7d. |

### 4.5 Common Mistakes

Same as vol_7d (Section 3.5), with these additions:

1. **Using ROWS BETWEEN 30 PRECEDING AND CURRENT ROW** (off by one): This creates a 31-row window. Correct: `29 PRECEDING AND CURRENT ROW`.
2. **Confusing calendar days with data rows**: If the data has gaps, a 30-row ROWS window might span 35 calendar days. For our gap-free daily dataset, this is not an issue, but document the assumption.

### 4.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | 0 to ~20 percentage points | vol_30d > 20 is rare even in crypto. The 30-day window smooths out extreme single-day moves. |
| Typical values | 1.0 to 5.0 for BTC; 1.5 to 8.0 for altcoins | Consistently near zero or near 20 indicates a problem. |
| NULL count per asset | Exactly 30 NULLs (1 for missing first return + 29 for insufficient window) | Off-by-one errors are the most common bug. |
| Smoother than vol_7d | vol_30d should change more gradually day-to-day | If vol_30d jumps as much as vol_7d, the window may be too small. |
| Regime detection | vol_30d should clearly distinguish bear market (high vol, Q4 2022) from accumulation (low vol, Q1 2023) | If vol_30d is flat across regimes, the computation is broken. |

### 4.7 Annualization Reference (NOT stored, for query-time use)

For any downstream query that needs annualized volatility:

```
annualized_vol = vol_30d * SQRT(365)
```

Example: If vol_30d = 3.5 percentage points, annualized vol = 3.5 * 19.105 = 66.9%.

Crypto annualization uses sqrt(365) because crypto trades every day. Do NOT use sqrt(252) -- that is for equities/TradFi which has ~252 trading days per year.

---

## 5. METRIC: `sma_7`

### 5.1 Definition

The 7-day simple moving average of the closing price. This is a trend indicator: when the current price is above sma_7, short-term momentum is bullish; below suggests bearish short-term pressure.

### 5.2 Formula

**Mathematical notation:**

```
SMA7_t = (1/7) * SUM_{i=t-6}^{t} P_i
```

Where:
- `P_i` = closing price on day i

**SQL pseudocode:**

```sql
sma_7 = CASE
    WHEN COUNT(close) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) = 7
    THEN AVG(close) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
    ELSE NULL
END
```

**Python pseudocode:**

```python
sma_7 = (
    df.groupby('asset_id')['close']
    .rolling(window=7, min_periods=7)
    .mean()
    .reset_index(level=0, drop=True)
)
```

### 5.3 Window Frame Specification

```sql
PARTITION BY asset_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
```

- 6 PRECEDING + CURRENT ROW = 7 rows.
- SMA is computed on close PRICES, not on returns. This is the standard definition.

### 5.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| Fewer than 7 closing prices | NULL | First valid sma_7 appears on the 7th trading day (index day 6, since we count from day 0). |
| Close prices vary wildly | Averaged normally | SMA is not affected by extreme values in any special way (unlike median). Large moves will shift the average, which is expected behavior. |
| All 7 closes identical | Equal to that close value | Valid. |

### 5.5 Common Mistakes

1. **Computing SMA on returns instead of prices**: SMA is a price-level indicator. Taking the SMA of returns is a different (less useful) metric.
2. **Off-by-one window size**: `ROWS BETWEEN 7 PRECEDING AND CURRENT ROW` gives 8 rows, not 7.
3. **Using RANGE instead of ROWS**: Same caution as volatility metrics. ROWS is predictable.
4. **Forgetting PARTITION BY asset_id**: SMA of BTC contaminated with ETH prices is nonsensical.
5. **Mixing up SMA and EMA**: Simple moving average weights all observations equally. Exponential moving average gives more weight to recent data. This project uses SMA, which is simpler and more transparent for a portfolio project.

### 5.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | Same order of magnitude as the close price for that asset | sma_7 for BTC should be in the tens-of-thousands range, not in single digits. |
| NULL count per asset | Exactly 6 NULLs (days 1-6 have insufficient data) | More or fewer NULLs indicates a threshold bug. |
| Smoothness | sma_7 should be smoother than raw close prices | If sma_7 exactly equals close every day, the window is probably 1 row. |
| Relationship to close | close oscillates around sma_7, crossing it frequently | If close is always above or below sma_7 for months, verify computation. |
| Cross-check | On any given day, sma_7 should be between the min and max of the last 7 close prices | If sma_7 is outside this range, the formula is wrong. |

---

## 6. METRIC: `sma_30`

### 6.1 Definition

The 30-day simple moving average of the closing price. A medium-term trend indicator. When price is above sma_30, the medium-term trend is bullish. The crossover of sma_7 above/below sma_30 is a commonly watched signal (the "golden cross" and "death cross" in simplified form).

### 6.2 Formula

**Mathematical notation:**

```
SMA30_t = (1/30) * SUM_{i=t-29}^{t} P_i
```

**SQL pseudocode:**

```sql
sma_30 = CASE
    WHEN COUNT(close) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) = 30
    THEN AVG(close) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )
    ELSE NULL
END
```

**Python pseudocode:**

```python
sma_30 = (
    df.groupby('asset_id')['close']
    .rolling(window=30, min_periods=30)
    .mean()
    .reset_index(level=0, drop=True)
)
```

### 6.3 Window Frame Specification

```sql
PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
```

- 29 PRECEDING + CURRENT ROW = 30 rows.

### 6.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| Fewer than 30 closing prices | NULL | First valid sma_30 appears on the 30th trading day. |
| Same notes as sma_7 | Apply analogously | All sma_7 edge cases apply, scaled to 30 days. |

### 6.5 Common Mistakes

Same as sma_7 (Section 5.5), with this addition:

1. **Comparing sma_7 and sma_30 before sma_30 is valid**: For the first 29 days, sma_30 is NULL. Do not compute sma_7/sma_30 crossover signals during this period.

### 6.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | Same order of magnitude as close price | Same as sma_7. |
| NULL count per asset | Exactly 29 NULLs | Off-by-one error is the most common bug. |
| Smoother than sma_7 | sma_30 should change even more gradually than sma_7 | If sma_30 is noisier than sma_7, the windows are swapped. |
| Lag | sma_30 should lag behind close price more than sma_7 during trending markets | During a rally, close > sma_7 > sma_30. During a decline, close < sma_7 < sma_30. |
| Cross-check | sma_30 should be between min and max of last 30 close prices | Same logic as sma_7. |

---

## 7. METRIC: `volume_ratio_30d`

### 7.1 Definition

Today's trading volume divided by the average daily volume over the preceding 30 calendar days (excluding the current day). A ratio of 1.0 means today's volume equals the recent average. A ratio of 2.0+ indicates a volume spike (anomaly). A ratio below 0.5 indicates unusually low volume.

### 7.2 Formula

**Mathematical notation:**

```
VR_t = V_t / ( (1/30) * SUM_{i=t-30}^{t-1} V_i )
```

Where:
- `V_t` = volume_usd on day t
- `V_i` = volume_usd on day i
- The denominator averages the 30 days BEFORE day t, excluding day t itself

**SQL pseudocode:**

```sql
volume_ratio_30d = CASE
    WHEN COUNT(volume_usd) OVER (
        PARTITION BY asset_id ORDER BY date
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) = 30
    THEN volume_usd / NULLIF(
        AVG(volume_usd) OVER (
            PARTITION BY asset_id ORDER BY date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ), 0
    )
    ELSE NULL
END
```

**Python pseudocode:**

```python
avg_vol_30d = (
    df.groupby('asset_id')['volume_usd']
    .rolling(window=30, min_periods=30)
    .mean()
    .shift(1)  # shift ensures we exclude the current day
    .reset_index(level=0, drop=True)
)
# Actually, a cleaner approach:
avg_vol_30d = (
    df.groupby('asset_id')['volume_usd']
    .transform(lambda x: x.shift(1).rolling(window=30, min_periods=30).mean())
)
volume_ratio_30d = df['volume_usd'] / avg_vol_30d
```

### 7.3 Window Frame Specification

```sql
PARTITION BY asset_id ORDER BY date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
```

- **30 PRECEDING to 1 PRECEDING**: This includes 30 rows ending at the row BEFORE the current row. The current row is explicitly excluded.
- This is the critical design choice (Decision 3): the denominator must not contain today's volume.

### 7.4 Edge Cases and NULL Rules

| Condition | Result | Explanation |
|-----------|--------|-------------|
| Fewer than 30 prior volume observations | NULL | The CASE expression returns NULL when the preceding window has fewer than 30 rows. First valid value appears on the 31st trading day. |
| Today's volume is zero | 0.0000 | A ratio of zero is valid. It means there was no trading today. |
| Average volume over prior 30 days is zero | NULL | Use NULLIF to convert a zero denominator to NULL, producing NULL instead of a division-by-zero error. This is astronomically unlikely for our 8 assets but is defensive coding. |
| Today's volume is NULL | NULL | Any arithmetic with NULL yields NULL. |
| Extreme volume spike (10x average) | 10.0000 | Valid. During the FTX collapse, some assets saw 5-10x normal volume. The tariff crash saw even higher multiples. |

### 7.5 Common Mistakes

1. **Including the current day in the denominator**: Using `ROWS BETWEEN 30 PRECEDING AND CURRENT ROW` instead of `30 PRECEDING AND 1 PRECEDING` contaminates the baseline with today's value. A massive volume spike would show a ratio of maybe 5x instead of 10x because the spike is averaged into the denominator.
2. **Off-by-one in the preceding window**: `ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING` gives only 29 prior days, not 30. The correct frame is `30 PRECEDING AND 1 PRECEDING` for exactly 30 prior rows.
3. **Not handling division by zero**: Even though it is unlikely, always wrap the denominator in NULLIF(..., 0).
4. **Using market_cap or close price instead of volume_usd**: The schema column is `volume_usd` in daily_prices.
5. **Forgetting PARTITION BY asset_id**: Volume levels differ enormously across assets (BTC volume >> DOT volume). Comparing one asset's volume to another's average is meaningless.
6. **Confusing volume_ratio with volume z-score**: The ratio divides volume by the mean. A z-score subtracts the mean and divides by the standard deviation. Both are valid for anomaly detection, but this project uses the ratio (simpler, more intuitive: "3x normal volume" is immediately understandable).

### 7.6 Validation Checks

| Check | Expected Range | Bug Indicator |
|-------|----------------|---------------|
| Value range | 0.01 to ~20.0 | A ratio of 0 means zero volume (possible but rare). A ratio above 20 is extraordinary -- verify against known events. |
| Typical values | 0.5 to 2.0 for most days | If most values cluster around exactly 1.0 with no variation, the computation may be using the same value in numerator and denominator. |
| Mean over full period | Slightly above 1.0 | The ratio has a positive skew (spikes are larger than dips). A mean of exactly 1.0 or below 0.8 suggests a bug. |
| NULL count per asset | Exactly 30 NULLs (first 30 days have insufficient prior data) | More or fewer NULLs indicates a threshold or window bug. |
| Spikes on known events | volume_ratio_30d > 2.0 during FTX collapse week, BTC ETF approval, tariff crash | If volume_ratio is flat during known high-volume events, the formula or data is wrong. |
| Never negative | volume_ratio_30d >= 0 always | Volume is non-negative. The ratio of non-negative values is non-negative. A negative value is impossible and indicates a bug. |

### 7.7 Anomaly Detection Thresholds (for downstream queries)

These thresholds are NOT enforced in the metrics computation; they are used by analytical queries in Phase 4:

| Threshold | Classification | Interpretation |
|-----------|---------------|----------------|
| volume_ratio_30d > 3.0 | Strong anomaly | Volume is 3x the 30-day average. Almost always corresponds to a market event. |
| volume_ratio_30d > 2.0 | Moderate anomaly | Volume is 2x the 30-day average. Common around significant news. |
| volume_ratio_30d > 1.5 | Mild elevation | Elevated but may not correspond to a specific event. |
| volume_ratio_30d < 0.5 | Unusually low | Volume dried up. Typical during weekends, holidays, or post-crash exhaustion. |
| volume_ratio_30d < 0.3 | Extreme low | Potential data issue or very unusual market condition. |

---

## 8. CROSS-METRIC CONSISTENCY CHECKS

After all 7 metrics are computed, run these cross-metric validation queries:

### 8.1 Date Alignment

```sql
-- Every row in daily_metrics should have a corresponding row in daily_prices
SELECT dm.asset_id, dm.date
FROM daily_metrics dm
LEFT JOIN daily_prices dp ON dm.asset_id = dp.asset_id AND dm.date = dp.date
WHERE dp.price_id IS NULL;
-- Expected: 0 rows
```

### 8.2 NULL Waterfall

For each asset, NULLs should follow a strict waterfall pattern:

| Day (1-indexed) | daily_return_pct | daily_range_pct | vol_7d | vol_30d | sma_7 | sma_30 | volume_ratio_30d |
|-----------------|-----------------|-----------------|--------|---------|-------|--------|------------------|
| Day 1 | NULL | VALUE | NULL | NULL | NULL | NULL | NULL |
| Day 2-6 | VALUE | VALUE | NULL | NULL | NULL | NULL | NULL |
| Day 7 | VALUE | VALUE | NULL | NULL | VALUE | NULL | NULL |
| Day 8 | VALUE | VALUE | VALUE | NULL | VALUE | NULL | NULL |
| Day 9-29 | VALUE | VALUE | VALUE | NULL | VALUE | NULL | NULL |
| Day 30 | VALUE | VALUE | VALUE | NULL | VALUE | VALUE | NULL |
| Day 31 | VALUE | VALUE | VALUE | VALUE | VALUE | VALUE | VALUE |
| Day 32+ | VALUE | VALUE | VALUE | VALUE | VALUE | VALUE | VALUE |

Explanation of the counts:
- **daily_return_pct**: 1 NULL (day 1 -- no previous close).
- **daily_range_pct**: 0 NULLs (only requires current day's high and low).
- **vol_7d**: 7 NULLs. Day 1 has no return. Days 2-7 have 1-6 returns. Day 8 is the first day with 7 returns in the window.
- **vol_30d**: 30 NULLs. Day 1 has no return. Days 2-30 have 1-29 returns. Day 31 is the first day with 30 returns.
- **sma_7**: 6 NULLs. Days 1-6 have fewer than 7 prices. Day 7 is the first day with 7 prices.
- **sma_30**: 29 NULLs. Days 1-29 have fewer than 30 prices. Day 30 has 30 prices.
- **volume_ratio_30d**: 30 NULLs. Days 1-30 have fewer than 30 prior volume observations. Day 31 is the first day with 30 prior rows.

```sql
-- Validate NULL counts per asset
SELECT
    asset_id,
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(daily_return_pct) AS null_return,
    COUNT(*) - COUNT(daily_range_pct) AS null_range,
    COUNT(*) - COUNT(vol_7d) AS null_vol7,
    COUNT(*) - COUNT(vol_30d) AS null_vol30,
    COUNT(*) - COUNT(sma_7) AS null_sma7,
    COUNT(*) - COUNT(sma_30) AS null_sma30,
    COUNT(*) - COUNT(volume_ratio_30d) AS null_volratio
FROM daily_metrics
GROUP BY asset_id
ORDER BY asset_id;
-- Expected: null_return=1, null_range=0, null_vol7=7, null_vol30=30,
--           null_sma7=6, null_sma30=29, null_volratio=30 for each asset
```

### 8.3 Volatility Relationship

```sql
-- vol_7d should be more variable than vol_30d
-- This checks that the standard deviation of vol_7d > standard deviation of vol_30d
SELECT
    asset_id,
    STDDEV_POP(vol_7d) AS variability_of_vol7d,
    STDDEV_POP(vol_30d) AS variability_of_vol30d
FROM daily_metrics
WHERE vol_7d IS NOT NULL AND vol_30d IS NOT NULL
GROUP BY asset_id;
-- Expected: variability_of_vol7d > variability_of_vol30d for all assets
```

### 8.4 SMA Ordering During Trends

```sql
-- During known bull run (Nov 2024 - Sep 2025), for BTC:
-- close > sma_7 > sma_30 should hold on most days
SELECT
    COUNT(*) AS total_days,
    COUNT(*) FILTER (WHERE dp.close > dm.sma_7 AND dm.sma_7 > dm.sma_30) AS bullish_aligned
FROM daily_metrics dm
JOIN daily_prices dp ON dm.asset_id = dp.asset_id AND dm.date = dp.date
JOIN assets a ON a.asset_id = dm.asset_id
WHERE a.symbol = 'BTC'
  AND dm.date BETWEEN '2024-11-06' AND '2025-09-30'
  AND dm.sma_30 IS NOT NULL;
-- Expected: bullish_aligned / total_days > 0.6
```

### 8.5 Volume Ratio Spike on Known Event

```sql
-- FTX collapse (Nov 8, 2022) should show volume_ratio_30d > 2.0 for most assets
SELECT
    a.symbol,
    dm.volume_ratio_30d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.date = '2022-11-08'
  AND dm.volume_ratio_30d IS NOT NULL
ORDER BY dm.volume_ratio_30d DESC;
-- Expected: most assets show ratio > 2.0, SOL should be among the highest
```

---

## 9. SCHEMA PRECISION VERIFICATION

Cross-referencing the formulas against the `daily_metrics` table column types:

| Column | Type | Max Value in Type | Expected Max Value | Verdict |
|--------|------|--------------------|--------------------|---------|
| daily_return_pct | NUMERIC(10,6) | 9999.999999 | ~100 (100% daily gain) | SAFE. Crypto has never seen a 10,000% single-day move for a major asset. |
| daily_range_pct | NUMERIC(10,6) | 9999.999999 | ~60 (60% intraday range) | SAFE. |
| vol_7d | NUMERIC(10,6) | 9999.999999 | ~30 | SAFE. |
| vol_30d | NUMERIC(10,6) | 9999.999999 | ~20 | SAFE. |
| sma_7 | NUMERIC(20,8) | 999999999999.99999999 | ~150,000 (BTC at peak) | SAFE. |
| sma_30 | NUMERIC(20,8) | 999999999999.99999999 | ~150,000 | SAFE. |
| volume_ratio_30d | NUMERIC(10,4) | 999999.9999 | ~20 | SAFE. Only 4 decimal places, but ratios do not need more precision. |

No schema changes are needed. All column types accommodate the expected value ranges with generous headroom.

---

## 10. IMPLEMENTATION SEQUENCE

The metrics have computation dependencies. They MUST be computed in this order:

```
Step 1: daily_return_pct  (depends on: close prices from daily_prices)
Step 2: daily_range_pct   (depends on: high, low from daily_prices)
                           -- Steps 1 and 2 can run in parallel

Step 3: vol_7d            (depends on: daily_return_pct from Step 1)
Step 4: vol_30d           (depends on: daily_return_pct from Step 1)
                           -- Steps 3 and 4 can run in parallel

Step 5: sma_7             (depends on: close prices from daily_prices)
Step 6: sma_30            (depends on: close prices from daily_prices)
                           -- Steps 5 and 6 can run in parallel with Steps 3-4

Step 7: volume_ratio_30d  (depends on: volume_usd from daily_prices)
                           -- Step 7 can run in parallel with Steps 3-6
```

In practice, if computing all metrics in a single SQL INSERT ... SELECT statement, the dependency chain is handled naturally because daily_return_pct is computed inline (as a LAG expression) and then referenced by STDDEV_POP. However, if using CTEs or subqueries, the CTE that computes daily_return_pct must precede the CTE that computes vol_7d/vol_30d.

**Recommended SQL approach**: A two-pass approach.

```
Pass 1: Compute daily_return_pct and daily_range_pct
        (INSERT into daily_metrics or UPDATE).

Pass 2: Compute vol_7d, vol_30d, sma_7, sma_30, volume_ratio_30d
        (UPDATE daily_metrics using window functions over the just-inserted data).
```

Or, use a single INSERT with a CTE chain:

```sql
WITH returns AS (
    -- compute daily_return_pct and daily_range_pct from daily_prices
),
all_metrics AS (
    -- compute vol_7d, vol_30d, sma_7, sma_30, volume_ratio_30d
    -- using returns.daily_return_pct for the volatility window functions
    -- and daily_prices.close / volume_usd for SMAs and volume ratio
)
INSERT INTO daily_metrics (...)
SELECT ... FROM all_metrics;
```

---

## 11. SUMMARY TABLE

| # | Metric | Input Source | Window Frame (SQL) | Min Obs | Unit | Stored As | NULL on Day 1? |
|---|--------|-------------|-------------------|---------|------|-----------|----------------|
| 1 | daily_return_pct | close (current + previous day) | N/A (LAG, not a frame) | 2 prices | Percentage points | NUMERIC(10,6) | YES |
| 2 | daily_range_pct | high, low (current day) | N/A (single-row calc) | 1 row with OHLC | Percentage points | NUMERIC(10,6) | NO |
| 3 | vol_7d | daily_return_pct (7 values) | ROWS BETWEEN 6 PRECEDING AND CURRENT ROW | 7 returns | Percentage points (daily, not annualized) | NUMERIC(10,6) | YES |
| 4 | vol_30d | daily_return_pct (30 values) | ROWS BETWEEN 29 PRECEDING AND CURRENT ROW | 30 returns | Percentage points (daily, not annualized) | NUMERIC(10,6) | YES |
| 5 | sma_7 | close (7 values) | ROWS BETWEEN 6 PRECEDING AND CURRENT ROW | 7 prices | USD (same as close price) | NUMERIC(20,8) | YES |
| 6 | sma_30 | close (30 values) | ROWS BETWEEN 29 PRECEDING AND CURRENT ROW | 30 prices | USD (same as close price) | NUMERIC(20,8) | YES |
| 7 | volume_ratio_30d | volume_usd (current + 30 prior) | ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING | 30 prior volumes | Dimensionless ratio | NUMERIC(10,4) | YES |

---

## SIGN-OFF

This deliverable provides the complete formula specification for all 7 metrics in the `daily_metrics` table. The four preliminary decisions (storage format, STDDEV variant, volume ratio denominator, minimum observations) are resolved and justified.

The SQL Expert and Python Expert may proceed with implementation using the exact formulas, window frames, NULL rules, and validation checks defined above.

**Next actions**:
- SQL Expert / Python Expert: Implement metrics computation following the formulas and CASE-based NULL enforcement
- SQL Expert / Python Expert: Run the cross-metric validation queries (Section 8) after initial data load
- SQL Expert / Python Expert: Verify NULL counts match the expected waterfall (Section 8.2)
- Finance Expert (Phase 3+): Validate computed values against known market events
