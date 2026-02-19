-- ============================================================================
-- Data Integrity Verification Queries -- Phase 2
-- File: 00_data_integrity_checks.sql
-- Phase: 2 -- Metrics Computation (post-validation)
-- Database: PostgreSQL
-- ============================================================================
--
-- PURPOSE
-- =======
-- This file contains a comprehensive set of data integrity queries that
-- validate the correctness of the daily_metrics table after it has been
-- populated by compute_metrics.py (or equivalent SQL INSERT pipeline).
--
-- These checks verify:
--   1. Row count alignment between daily_prices and daily_metrics
--   2. NULL waterfall pattern matches the expected minimum-observation rules
--   3. Value ranges are physically plausible for crypto market data
--   4. Cross-metric consistency (volatility relationships, SMA ordering)
--   5. Spot checks against known historical market events
--
-- WHEN TO RUN
-- ===========
-- Run this file AFTER compute_metrics.py completes successfully and the
-- daily_metrics table is fully populated. Every query includes its expected
-- result in the comment block. If any query returns unexpected results,
-- investigate the metrics computation pipeline before proceeding to Phase 3.
--
-- USAGE
-- =====
-- Execute the entire file against your PostgreSQL database:
--   psql -d crypto_analytics -f queries/00_data_integrity_checks.sql
--
-- Or run individual sections by copying the desired query into your client.
--
-- Each query is self-contained and labeled with a section number and check ID.
-- Queries that should return ZERO rows (pass = empty result) are noted.
-- Queries that return summary data include expected value guidance.
--
-- ============================================================================


-- ############################################################################
-- SECTION 1: ROW COUNT ALIGNMENT
-- ############################################################################
-- daily_metrics and daily_prices share the same grain: one row per
-- (asset_id, date). After metrics computation, both tables must have
-- identical row counts and identical (asset_id, date) key sets.
-- ############################################################################


-- ----------------------------------------------------------------------------
-- CHECK 1.1: Total row counts must match
-- ----------------------------------------------------------------------------
-- Compares the total number of rows in daily_prices vs daily_metrics.
-- Expected: both counts are equal. A mismatch means the metrics pipeline
-- inserted too few or too many rows.
-- ----------------------------------------------------------------------------

SELECT
    'daily_prices'  AS table_name,
    COUNT(*)        AS row_count
FROM daily_prices

UNION ALL

SELECT
    'daily_metrics' AS table_name,
    COUNT(*)        AS row_count
FROM daily_metrics;

-- EXPECTED: Both row_count values are identical.


-- ----------------------------------------------------------------------------
-- CHECK 1.2: Per-asset row counts must match
-- ----------------------------------------------------------------------------
-- Breaks down the row count comparison by asset. This detects cases where
-- the totals happen to match but individual assets are misaligned (e.g.,
-- extra rows for BTC and missing rows for SOL, canceling out in the total).
-- ----------------------------------------------------------------------------

SELECT
    dp.asset_id,
    a.symbol,
    dp.price_rows,
    dm.metric_rows,
    dp.price_rows - dm.metric_rows AS row_difference
FROM (
    SELECT asset_id, COUNT(*) AS price_rows
    FROM daily_prices
    GROUP BY asset_id
) dp
JOIN (
    SELECT asset_id, COUNT(*) AS metric_rows
    FROM daily_metrics
    GROUP BY asset_id
) dm ON dp.asset_id = dm.asset_id
JOIN assets a ON a.asset_id = dp.asset_id
ORDER BY a.symbol;

-- EXPECTED: row_difference = 0 for every asset.


-- ----------------------------------------------------------------------------
-- CHECK 1.3: Every (asset_id, date) in daily_prices has a matching row
--            in daily_metrics
-- ----------------------------------------------------------------------------
-- Uses a LEFT JOIN to find orphaned daily_prices rows with no metrics.
-- Adapted from Phase 2 formula validation Section 8.1 (reversed direction
-- is checked in 1.4).
-- ----------------------------------------------------------------------------

SELECT
    dp.asset_id,
    a.symbol,
    dp.date
FROM daily_prices dp
LEFT JOIN daily_metrics dm
    ON dp.asset_id = dm.asset_id
    AND dp.date = dm.date
JOIN assets a ON a.asset_id = dp.asset_id
WHERE dm.metric_id IS NULL
ORDER BY dp.asset_id, dp.date;

-- EXPECTED: 0 rows. Every price row should have a corresponding metrics row.


-- ----------------------------------------------------------------------------
-- CHECK 1.4: Every (asset_id, date) in daily_metrics has a matching row
--            in daily_prices
-- ----------------------------------------------------------------------------
-- The reverse check: finds orphaned daily_metrics rows with no price data.
-- This would indicate metrics were computed for dates that have no underlying
-- price data -- a serious pipeline bug.
-- Sourced from Phase 2 formula validation Section 8.1.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date
FROM daily_metrics dm
LEFT JOIN daily_prices dp
    ON dm.asset_id = dp.asset_id
    AND dm.date = dp.date
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dp.price_id IS NULL
ORDER BY dm.asset_id, dm.date;

-- EXPECTED: 0 rows. Every metrics row must trace back to a price row.


-- ############################################################################
-- SECTION 2: NULL WATERFALL VERIFICATION
-- ############################################################################
-- Each metric has a precise number of expected NULLs per asset, governed by
-- the minimum-observation rules defined in Phase 2 formula validation:
--
--   daily_return_pct  :  1 NULL per asset (first day -- no previous close)
--   daily_range_pct   :  0 NULLs (only needs current day's high and low)
--   vol_7d            :  7 NULLs per asset (day 1 has no return;
--                         days 2-7 have < 7 returns)
--   vol_30d           : 30 NULLs per asset (day 1 has no return;
--                         days 2-30 have < 30 returns)
--   sma_7             :  6 NULLs per asset (days 1-6 have < 7 prices)
--   sma_30            : 29 NULLs per asset (days 1-29 have < 30 prices)
--   volume_ratio_30d  : 30 NULLs per asset (days 1-30 have < 30 prior
--                         volume observations)
--
-- Source: Phase 2 formula validation Section 8.2 NULL waterfall table.
-- ############################################################################


-- ----------------------------------------------------------------------------
-- CHECK 2.1: Aggregate NULL counts per asset
-- ----------------------------------------------------------------------------
-- Counts the number of NULL values for each metric, per asset.
-- The results should exactly match the expected NULL counts above.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    COUNT(*)                                        AS total_rows,
    COUNT(*) - COUNT(daily_return_pct)              AS null_daily_return_pct,
    COUNT(*) - COUNT(daily_range_pct)               AS null_daily_range_pct,
    COUNT(*) - COUNT(vol_7d)                        AS null_vol_7d,
    COUNT(*) - COUNT(vol_30d)                       AS null_vol_30d,
    COUNT(*) - COUNT(sma_7)                         AS null_sma_7,
    COUNT(*) - COUNT(sma_30)                        AS null_sma_30,
    COUNT(*) - COUNT(volume_ratio_30d)              AS null_volume_ratio_30d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
GROUP BY dm.asset_id, a.symbol
ORDER BY a.symbol;

-- EXPECTED per asset:
--   null_daily_return_pct =  1
--   null_daily_range_pct  =  0
--   null_vol_7d           =  7
--   null_vol_30d          = 30
--   null_sma_7            =  6
--   null_sma_30           = 29
--   null_volume_ratio_30d = 30


-- ----------------------------------------------------------------------------
-- CHECK 2.2: Flag assets with unexpected NULL counts
-- ----------------------------------------------------------------------------
-- Returns only assets where any metric's NULL count deviates from the
-- expected value. An empty result means all assets pass.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    COUNT(*)                                        AS total_rows,
    COUNT(*) - COUNT(daily_return_pct)              AS null_return,
    COUNT(*) - COUNT(daily_range_pct)               AS null_range,
    COUNT(*) - COUNT(vol_7d)                        AS null_vol7,
    COUNT(*) - COUNT(vol_30d)                       AS null_vol30,
    COUNT(*) - COUNT(sma_7)                         AS null_sma7,
    COUNT(*) - COUNT(sma_30)                        AS null_sma30,
    COUNT(*) - COUNT(volume_ratio_30d)              AS null_volratio
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
GROUP BY dm.asset_id, a.symbol
HAVING
    COUNT(*) - COUNT(daily_return_pct)  != 1
    OR COUNT(*) - COUNT(daily_range_pct)   != 0
    OR COUNT(*) - COUNT(vol_7d)            != 7
    OR COUNT(*) - COUNT(vol_30d)           != 30
    OR COUNT(*) - COUNT(sma_7)             != 6
    OR COUNT(*) - COUNT(sma_30)            != 29
    OR COUNT(*) - COUNT(volume_ratio_30d)  != 30
ORDER BY a.symbol;

-- EXPECTED: 0 rows. Any rows returned indicate a NULL count anomaly.


-- ----------------------------------------------------------------------------
-- CHECK 2.3: Verify NULLs are in the EARLIEST rows (not scattered)
-- ----------------------------------------------------------------------------
-- For each asset, the NULL rows for daily_return_pct should be exactly the
-- first date. NULLs for vol_30d should be the first 30 dates. This query
-- verifies that NULLs appear at the beginning of each asset's time series
-- and not randomly scattered throughout.
-- ----------------------------------------------------------------------------

WITH ranked AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date,
        ROW_NUMBER() OVER (
            PARTITION BY dm.asset_id ORDER BY dm.date
        ) AS day_num,
        daily_return_pct,
        daily_range_pct,
        vol_7d,
        vol_30d,
        sma_7,
        sma_30,
        volume_ratio_30d
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
)
SELECT
    asset_id,
    symbol,
    date,
    day_num,
    CASE WHEN daily_return_pct IS NULL THEN 'NULL' ELSE 'VALUE' END AS return_status,
    CASE WHEN vol_7d IS NULL            THEN 'NULL' ELSE 'VALUE' END AS vol7_status,
    CASE WHEN vol_30d IS NULL           THEN 'NULL' ELSE 'VALUE' END AS vol30_status,
    CASE WHEN sma_7 IS NULL            THEN 'NULL' ELSE 'VALUE' END AS sma7_status,
    CASE WHEN sma_30 IS NULL           THEN 'NULL' ELSE 'VALUE' END AS sma30_status,
    CASE WHEN volume_ratio_30d IS NULL THEN 'NULL' ELSE 'VALUE' END AS volratio_status
FROM ranked
WHERE
    -- Flag unexpected: NULL after the expected cutoff day
    (day_num >  1 AND daily_return_pct IS NULL)
    OR (day_num >  7 AND vol_7d IS NULL)
    OR (day_num > 30 AND vol_30d IS NULL)
    OR (day_num >  6 AND sma_7 IS NULL)
    OR (day_num > 29 AND sma_30 IS NULL)
    OR (day_num > 30 AND volume_ratio_30d IS NULL)
    -- Flag unexpected: VALUE before the expected cutoff day
    OR (day_num =  1 AND daily_return_pct IS NOT NULL)
    OR (day_num <= 7 AND vol_7d IS NOT NULL)
    OR (day_num <= 30 AND vol_30d IS NOT NULL)
    OR (day_num <= 6 AND sma_7 IS NOT NULL)
    OR (day_num <= 29 AND sma_30 IS NOT NULL)
    OR (day_num <= 30 AND volume_ratio_30d IS NOT NULL)
ORDER BY asset_id, day_num;

-- EXPECTED: 0 rows. Any rows indicate NULLs in wrong positions.


-- ############################################################################
-- SECTION 3: VALUE RANGE SANITY
-- ############################################################################
-- These checks verify that computed metric values fall within physically
-- plausible ranges for crypto market data. Values outside these ranges
-- indicate either a computation bug or a data quality issue in daily_prices.
-- ############################################################################


-- ----------------------------------------------------------------------------
-- CHECK 3.1: daily_return_pct range (-100 to +500)
-- ----------------------------------------------------------------------------
-- A return of -100% means the price went to zero (impossible for our assets
-- given the CHECK close > 0 constraint). A return above +500% in a single
-- day is implausible for any major cryptocurrency.
-- Source: Phase 2 formula validation Section 1.5.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date,
    dm.daily_return_pct,
    CASE
        WHEN dm.daily_return_pct <= -100 THEN 'VIOLATION: return <= -100%'
        WHEN dm.daily_return_pct >  500  THEN 'VIOLATION: return > +500%'
        ELSE 'OK'
    END AS range_check
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.daily_return_pct IS NOT NULL
  AND (dm.daily_return_pct <= -100 OR dm.daily_return_pct > 500)
ORDER BY ABS(dm.daily_return_pct) DESC;

-- EXPECTED: 0 rows. Any rows indicate extreme outliers requiring investigation.


-- ----------------------------------------------------------------------------
-- CHECK 3.2: daily_range_pct must be non-negative
-- ----------------------------------------------------------------------------
-- daily_range_pct = (high - low) / low * 100. Since high >= low (enforced
-- by market data logic), this must always be >= 0. A negative value means
-- high < low, which is a data error.
-- Source: Phase 2 formula validation Section 2.4 and 2.6.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date,
    dm.daily_range_pct,
    dp.high,
    dp.low
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
JOIN daily_prices dp ON dp.asset_id = dm.asset_id AND dp.date = dm.date
WHERE dm.daily_range_pct IS NOT NULL
  AND dm.daily_range_pct < 0
ORDER BY dm.daily_range_pct ASC;

-- EXPECTED: 0 rows. Any negative range indicates high < low data corruption.


-- ----------------------------------------------------------------------------
-- CHECK 3.3: Volatility metrics must be non-negative
-- ----------------------------------------------------------------------------
-- vol_7d and vol_30d are population standard deviations, which by
-- definition are >= 0. A negative value indicates a computation error.
-- Source: Phase 2 formula validation Sections 3.6 and 4.6.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date,
    dm.vol_7d,
    dm.vol_30d,
    CASE
        WHEN dm.vol_7d  < 0 THEN 'VIOLATION: vol_7d < 0'
        WHEN dm.vol_30d < 0 THEN 'VIOLATION: vol_30d < 0'
        ELSE 'MULTIPLE'
    END AS violation_type
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE (dm.vol_7d IS NOT NULL AND dm.vol_7d < 0)
   OR (dm.vol_30d IS NOT NULL AND dm.vol_30d < 0)
ORDER BY dm.asset_id, dm.date;

-- EXPECTED: 0 rows. Negative standard deviation is mathematically impossible.


-- ----------------------------------------------------------------------------
-- CHECK 3.4: SMA values must be positive (prices are always positive)
-- ----------------------------------------------------------------------------
-- sma_7 and sma_30 are averages of closing prices. Since the schema
-- enforces close > 0, an average of positive values must also be positive.
-- Source: Phase 2 formula validation Sections 5.6 and 6.6.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date,
    dm.sma_7,
    dm.sma_30,
    CASE
        WHEN dm.sma_7  <= 0 THEN 'VIOLATION: sma_7 <= 0'
        WHEN dm.sma_30 <= 0 THEN 'VIOLATION: sma_30 <= 0'
        ELSE 'MULTIPLE'
    END AS violation_type
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE (dm.sma_7 IS NOT NULL AND dm.sma_7 <= 0)
   OR (dm.sma_30 IS NOT NULL AND dm.sma_30 <= 0)
ORDER BY dm.asset_id, dm.date;

-- EXPECTED: 0 rows. Average of positive prices cannot be zero or negative.


-- ----------------------------------------------------------------------------
-- CHECK 3.5: volume_ratio_30d must be non-negative
-- ----------------------------------------------------------------------------
-- volume_ratio_30d = today's volume / avg(prior 30 days volume). Both
-- numerator and denominator are non-negative (volume >= 0), so the ratio
-- must also be >= 0.
-- Source: Phase 2 formula validation Section 7.6.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    dm.date,
    dm.volume_ratio_30d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.volume_ratio_30d IS NOT NULL
  AND dm.volume_ratio_30d < 0
ORDER BY dm.asset_id, dm.date;

-- EXPECTED: 0 rows. Ratio of non-negative values cannot be negative.


-- ----------------------------------------------------------------------------
-- CHECK 3.6: Summary statistics for all metrics (informational)
-- ----------------------------------------------------------------------------
-- Provides min, max, average, and percentile values for each metric across
-- the entire dataset. Use this to sanity-check overall distributions.
-- This is an informational query -- review the output manually.
-- ----------------------------------------------------------------------------

SELECT
    a.symbol,
    -- daily_return_pct
    MIN(dm.daily_return_pct)                            AS return_min,
    MAX(dm.daily_return_pct)                            AS return_max,
    ROUND(AVG(dm.daily_return_pct), 4)                  AS return_avg,
    -- daily_range_pct
    MIN(dm.daily_range_pct)                             AS range_min,
    MAX(dm.daily_range_pct)                             AS range_max,
    ROUND(AVG(dm.daily_range_pct), 4)                   AS range_avg,
    -- vol_7d
    MIN(dm.vol_7d)                                      AS vol7_min,
    MAX(dm.vol_7d)                                      AS vol7_max,
    ROUND(AVG(dm.vol_7d), 4)                            AS vol7_avg,
    -- vol_30d
    MIN(dm.vol_30d)                                     AS vol30_min,
    MAX(dm.vol_30d)                                     AS vol30_max,
    ROUND(AVG(dm.vol_30d), 4)                           AS vol30_avg,
    -- sma_7
    MIN(dm.sma_7)                                       AS sma7_min,
    MAX(dm.sma_7)                                       AS sma7_max,
    -- sma_30
    MIN(dm.sma_30)                                      AS sma30_min,
    MAX(dm.sma_30)                                      AS sma30_max,
    -- volume_ratio_30d
    MIN(dm.volume_ratio_30d)                            AS volratio_min,
    MAX(dm.volume_ratio_30d)                            AS volratio_max,
    ROUND(AVG(dm.volume_ratio_30d), 4)                  AS volratio_avg
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
GROUP BY a.symbol
ORDER BY a.symbol;

-- EXPECTED (manual review -- approximate guidance):
--   return_avg    : Between -0.5 and +0.5 per asset
--   range_avg     : Between 1.0 and 15.0 for most assets
--   vol7_avg      : Between 0.5 and 10.0
--   vol30_avg     : Between 0.5 and 8.0
--   sma7/sma30    : Same order of magnitude as the asset's price
--   volratio_avg  : Slightly above 1.0


-- ############################################################################
-- SECTION 4: CROSS-METRIC CONSISTENCY
-- ############################################################################
-- These checks verify relationships that should hold between different
-- metrics. They are derived from financial first principles and the
-- Phase 2 formula validation Section 8 cross-metric checks.
-- ############################################################################


-- ----------------------------------------------------------------------------
-- CHECK 4.1: vol_7d variability should exceed vol_30d variability
-- ----------------------------------------------------------------------------
-- A shorter rolling window (7 days) reacts more sharply to individual
-- extreme days, so the standard deviation of vol_7d values over time
-- should be larger than the standard deviation of vol_30d values.
-- Sourced directly from Phase 2 formula validation Section 8.3.
-- ----------------------------------------------------------------------------

SELECT
    dm.asset_id,
    a.symbol,
    ROUND(STDDEV_POP(dm.vol_7d)::NUMERIC, 6)   AS variability_of_vol_7d,
    ROUND(STDDEV_POP(dm.vol_30d)::NUMERIC, 6)  AS variability_of_vol_30d,
    CASE
        WHEN STDDEV_POP(dm.vol_7d) > STDDEV_POP(dm.vol_30d) THEN 'PASS'
        ELSE 'FAIL -- vol_7d should be noisier than vol_30d'
    END AS check_result
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.vol_7d IS NOT NULL AND dm.vol_30d IS NOT NULL
GROUP BY dm.asset_id, a.symbol
ORDER BY a.symbol;

-- EXPECTED: check_result = 'PASS' for all assets.
-- If vol_30d is noisier than vol_7d, the window sizes may be swapped
-- or the computation is incorrect.


-- ----------------------------------------------------------------------------
-- CHECK 4.2: SMA ordering during known BTC bull run (Nov 2024 - Sep 2025)
-- ----------------------------------------------------------------------------
-- During a sustained uptrend, the close price leads the moving averages:
--   close > sma_7 > sma_30
-- This "bullish alignment" should hold on the majority of days during
-- the Nov 2024 to Sep 2025 BTC bull run.
-- Sourced directly from Phase 2 formula validation Section 8.4.
-- ----------------------------------------------------------------------------

SELECT
    COUNT(*) AS total_days,
    COUNT(*) FILTER (
        WHERE dp.close > dm.sma_7 AND dm.sma_7 > dm.sma_30
    ) AS bullish_aligned_days,
    ROUND(
        COUNT(*) FILTER (
            WHERE dp.close > dm.sma_7 AND dm.sma_7 > dm.sma_30
        )::NUMERIC / COUNT(*)::NUMERIC * 100,
        1
    ) AS bullish_pct
FROM daily_metrics dm
JOIN daily_prices dp
    ON dm.asset_id = dp.asset_id AND dm.date = dp.date
JOIN assets a ON a.asset_id = dm.asset_id
WHERE a.symbol = 'BTC'
  AND dm.date BETWEEN '2024-11-06' AND '2025-09-30'
  AND dm.sma_30 IS NOT NULL;

-- EXPECTED: bullish_pct > 60%.
-- During a strong bull run, close > sma_7 > sma_30 should hold on the
-- majority of days. A percentage below 50% suggests the SMAs or close
-- prices may be wrong.


-- ----------------------------------------------------------------------------
-- CHECK 4.3: Volume ratio spike on FTX collapse (2022-11-08)
-- ----------------------------------------------------------------------------
-- The FTX exchange collapse on November 8, 2022 was a major market event
-- that caused panic selling across all crypto assets. Trading volume spiked
-- dramatically, and volume_ratio_30d should be > 2.0 for most assets.
-- Sourced directly from Phase 2 formula validation Section 8.5.
-- ----------------------------------------------------------------------------

SELECT
    a.symbol,
    dm.volume_ratio_30d,
    CASE
        WHEN dm.volume_ratio_30d > 2.0 THEN 'PASS -- spike detected'
        WHEN dm.volume_ratio_30d > 1.5 THEN 'MARGINAL -- elevated but < 2x'
        ELSE 'FAIL -- no spike detected on FTX collapse date'
    END AS check_result
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.date = '2022-11-08'
  AND dm.volume_ratio_30d IS NOT NULL
ORDER BY dm.volume_ratio_30d DESC;

-- EXPECTED: Most assets show volume_ratio_30d > 2.0.
-- SOL should be among the highest (FTX held large SOL positions).
-- If all ratios are near 1.0, the volume data or ratio formula is suspect.


-- ----------------------------------------------------------------------------
-- CHECK 4.4: vol_7d and vol_30d long-run means should be comparable
-- ----------------------------------------------------------------------------
-- While vol_7d is noisier day-to-day (Check 4.1), the average of vol_7d
-- over the full dataset should be in the same ballpark as the average of
-- vol_30d. If vol_7d is systematically 3x or more larger than vol_30d,
-- there is likely a window-size or formula error.
-- Source: Phase 2 formula validation Section 3.6.
-- ----------------------------------------------------------------------------

SELECT
    a.symbol,
    ROUND(AVG(dm.vol_7d)::NUMERIC, 4)   AS avg_vol_7d,
    ROUND(AVG(dm.vol_30d)::NUMERIC, 4)  AS avg_vol_30d,
    ROUND(
        (AVG(dm.vol_7d) / NULLIF(AVG(dm.vol_30d), 0))::NUMERIC,
        2
    ) AS ratio_of_means,
    CASE
        WHEN AVG(dm.vol_7d) / NULLIF(AVG(dm.vol_30d), 0) BETWEEN 0.5 AND 2.0
            THEN 'PASS'
        ELSE 'FAIL -- means diverge too much'
    END AS check_result
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.vol_7d IS NOT NULL AND dm.vol_30d IS NOT NULL
GROUP BY a.symbol
ORDER BY a.symbol;

-- EXPECTED: ratio_of_means between 0.5 and 2.0 for all assets.
-- Typically the ratio should be close to 1.0. A ratio of 3.0+ or 0.3-
-- suggests a formula or window error.


-- ----------------------------------------------------------------------------
-- CHECK 4.5: SMA values are bounded by recent close price range
-- ----------------------------------------------------------------------------
-- On any given day, sma_7 must fall between the minimum and maximum close
-- prices of the prior 7 days (the SMA is their average). Similarly for
-- sma_30. This is a mathematical invariant.
-- Source: Phase 2 formula validation Sections 5.6 and 6.6.
-- ----------------------------------------------------------------------------

WITH sma_bounds AS (
    SELECT
        dp.asset_id,
        dp.date,
        dp.close,
        MIN(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS min_close_7,
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS max_close_7,
        MIN(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS min_close_30,
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS max_close_30
    FROM daily_prices dp
)
SELECT
    sb.asset_id,
    a.symbol,
    sb.date,
    dm.sma_7,
    sb.min_close_7,
    sb.max_close_7,
    dm.sma_30,
    sb.min_close_30,
    sb.max_close_30
FROM sma_bounds sb
JOIN daily_metrics dm ON dm.asset_id = sb.asset_id AND dm.date = sb.date
JOIN assets a ON a.asset_id = sb.asset_id
WHERE
    (dm.sma_7 IS NOT NULL AND (dm.sma_7 < sb.min_close_7 OR dm.sma_7 > sb.max_close_7))
    OR
    (dm.sma_30 IS NOT NULL AND (dm.sma_30 < sb.min_close_30 OR dm.sma_30 > sb.max_close_30))
ORDER BY sb.asset_id, sb.date;

-- EXPECTED: 0 rows. An average of a set of numbers must lie between
-- the minimum and maximum of that set. Any rows indicate a computation bug.


-- ############################################################################
-- SECTION 5: KNOWN EVENT SPOT CHECKS
-- ############################################################################
-- These checks verify that specific metric values match expected approximate
-- values on dates of known market events. Spot checks provide a "sanity
-- smell test" that the data and formulas are grounded in reality.
-- ############################################################################


-- ----------------------------------------------------------------------------
-- CHECK 5.1: BTC daily_return_pct on known dates
-- ----------------------------------------------------------------------------
-- Spot-check BTC daily returns against well-documented market events.
-- Approximate expected values are based on publicly known BTC price moves.
--
-- Known dates and approximate BTC moves:
--   2022-06-13: Celsius/3AC contagion panic, BTC dropped ~15% in one day
--   2022-11-08: FTX collapse announcement, BTC dropped ~10-14%
--   2023-10-24: BlackRock BTC ETF ticker appeared on DTCC, BTC surged ~10%
--   2024-11-06: U.S. election result (pro-crypto candidate), BTC surged ~8%
-- ----------------------------------------------------------------------------

SELECT
    dm.date,
    dm.daily_return_pct,
    dp.close,
    CASE
        WHEN dm.date = '2022-06-13' THEN 'Celsius/3AC panic: expected ~ -15% to -20%'
        WHEN dm.date = '2022-11-08' THEN 'FTX collapse: expected ~ -10% to -14%'
        WHEN dm.date = '2023-10-24' THEN 'ETF DTCC listing: expected ~ +7% to +15%'
        WHEN dm.date = '2024-11-06' THEN 'U.S. election: expected ~ +6% to +10%'
        ELSE 'unknown'
    END AS event_context
FROM daily_metrics dm
JOIN daily_prices dp ON dp.asset_id = dm.asset_id AND dp.date = dm.date
JOIN assets a ON a.asset_id = dm.asset_id
WHERE a.symbol = 'BTC'
  AND dm.date IN ('2022-06-13', '2022-11-08', '2023-10-24', '2024-11-06')
ORDER BY dm.date;

-- EXPECTED (approximate):
--   2022-06-13: daily_return_pct around -15% to -20%
--   2022-11-08: daily_return_pct around -10% to -14%
--   2023-10-24: daily_return_pct around +7% to +15%
--   2024-11-06: daily_return_pct around +6% to +10%
--
-- If the signs are wrong (positive on crash dates, negative on rally dates),
-- the return formula is inverted. If the magnitudes are orders of magnitude
-- off, the *100 multiplier may be missing or applied twice.


-- ----------------------------------------------------------------------------
-- CHECK 5.2: BTC vol_30d regime check -- FTX vs accumulation periods
-- ----------------------------------------------------------------------------
-- vol_30d should clearly distinguish between high-volatility regimes
-- (e.g., FTX collapse in Nov 2022) and low-volatility accumulation
-- periods (e.g., mid-2023 when BTC traded sideways around $26-30K).
-- Source: Phase 2 formula validation Section 4.6.
-- ----------------------------------------------------------------------------

SELECT
    period_label,
    ROUND(AVG(dm.vol_30d)::NUMERIC, 4) AS avg_vol_30d,
    ROUND(MIN(dm.vol_30d)::NUMERIC, 4) AS min_vol_30d,
    ROUND(MAX(dm.vol_30d)::NUMERIC, 4) AS max_vol_30d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN dm.date BETWEEN '2022-11-01' AND '2022-11-30'
                THEN 'Nov 2022 (FTX crash -- HIGH vol expected)'
            WHEN dm.date BETWEEN '2023-06-01' AND '2023-08-31'
                THEN 'Jun-Aug 2023 (accumulation -- LOW vol expected)'
            ELSE NULL
        END AS period_label
) pl
WHERE a.symbol = 'BTC'
  AND dm.vol_30d IS NOT NULL
  AND pl.period_label IS NOT NULL
GROUP BY period_label
ORDER BY period_label;

-- EXPECTED:
--   Nov 2022 (FTX crash): avg_vol_30d significantly HIGHER than
--   Jun-Aug 2023 (accumulation): avg_vol_30d.
--
-- The FTX period vol should be at least 1.5x to 3x the accumulation
-- period vol. If both periods show similar vol, the vol_30d computation
-- is not capturing regime differences.


-- ----------------------------------------------------------------------------
-- CHECK 5.3: Multi-asset return check on FTX collapse date (2022-11-08)
-- ----------------------------------------------------------------------------
-- Nearly all major crypto assets declined on the FTX collapse date.
-- This verifies that returns are negative across the board and that
-- SOL (closely tied to FTX) shows one of the worst returns.
-- ----------------------------------------------------------------------------

SELECT
    a.symbol,
    dm.daily_return_pct,
    dm.daily_range_pct,
    dm.volume_ratio_30d,
    CASE
        WHEN dm.daily_return_pct < -5 THEN 'PASS -- significant decline'
        WHEN dm.daily_return_pct < 0   THEN 'MARGINAL -- small decline'
        ELSE 'FAIL -- positive return on crash date is suspicious'
    END AS return_check,
    CASE
        WHEN dm.volume_ratio_30d > 2.0 THEN 'PASS -- volume spike'
        ELSE 'FAIL -- no volume spike'
    END AS volume_check
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.date = '2022-11-08'
ORDER BY dm.daily_return_pct ASC;

-- EXPECTED:
--   Most assets show daily_return_pct < -5%
--   SOL should show one of the largest declines (was closely tied to FTX)
--   daily_range_pct should be elevated (high intraday volatility)
--   volume_ratio_30d should be > 2.0 for most assets


-- ----------------------------------------------------------------------------
-- CHECK 5.4: BTC daily_range_pct spike during FTX collapse week
-- ----------------------------------------------------------------------------
-- Intraday volatility (daily_range_pct) should be visibly elevated during
-- the week of the FTX collapse compared to the prior calm period.
-- ----------------------------------------------------------------------------

SELECT
    dm.date,
    dm.daily_range_pct,
    dm.daily_return_pct,
    dm.vol_7d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE a.symbol = 'BTC'
  AND dm.date BETWEEN '2022-11-06' AND '2022-11-14'
ORDER BY dm.date;

-- EXPECTED: daily_range_pct values during this week should be noticeably
-- higher than the typical 2-5% range for BTC. Values of 8-15%+ on the
-- peak crash days (Nov 8-9) would be normal. vol_7d should also be
-- elevated by the end of this window as the crash returns enter the
-- rolling 7-day window.


-- ----------------------------------------------------------------------------
-- CHECK 5.5: Verify daily_return_pct sign matches price direction
-- ----------------------------------------------------------------------------
-- For a random sample of rows, verify that the sign of daily_return_pct
-- is consistent with the actual price movement (close vs previous close).
-- This catches formula inversion bugs (e.g., using P_{t-1}/P_t instead
-- of P_t/P_{t-1}).
-- ----------------------------------------------------------------------------

WITH price_with_prev AS (
    SELECT
        dp.asset_id,
        dp.date,
        dp.close,
        LAG(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
        ) AS prev_close
    FROM daily_prices dp
)
SELECT
    a.symbol,
    pw.date,
    pw.prev_close,
    pw.close,
    dm.daily_return_pct,
    CASE
        WHEN pw.close > pw.prev_close AND dm.daily_return_pct > 0 THEN 'OK'
        WHEN pw.close < pw.prev_close AND dm.daily_return_pct < 0 THEN 'OK'
        WHEN pw.close = pw.prev_close AND dm.daily_return_pct = 0 THEN 'OK'
        ELSE 'MISMATCH -- sign does not match price direction'
    END AS sign_check
FROM price_with_prev pw
JOIN daily_metrics dm ON dm.asset_id = pw.asset_id AND dm.date = pw.date
JOIN assets a ON a.asset_id = pw.asset_id
WHERE pw.prev_close IS NOT NULL
  AND dm.daily_return_pct IS NOT NULL
  AND (
      (pw.close > pw.prev_close AND dm.daily_return_pct <= 0)
      OR (pw.close < pw.prev_close AND dm.daily_return_pct >= 0)
  )
ORDER BY a.symbol, pw.date
LIMIT 50;

-- EXPECTED: 0 rows. If the return is positive when price declined (or
-- vice versa), the return formula is inverted or using the wrong column.
-- NOTE: Rows where close = prev_close and return = 0 are fine and excluded.


-- ----------------------------------------------------------------------------
-- CHECK 5.6: Verify daily_return_pct magnitude via direct recomputation
-- ----------------------------------------------------------------------------
-- Recompute daily_return_pct independently from daily_prices and compare
-- to the stored value. Any discrepancy beyond rounding tolerance indicates
-- a formula bug in the metrics computation pipeline.
-- ----------------------------------------------------------------------------

WITH recomputed AS (
    SELECT
        dp.asset_id,
        dp.date,
        dp.close,
        LAG(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
        ) AS prev_close,
        (dp.close - LAG(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
        )) / LAG(dp.close) OVER (
            PARTITION BY dp.asset_id ORDER BY dp.date
        ) * 100 AS expected_return_pct
    FROM daily_prices dp
)
SELECT
    a.symbol,
    r.date,
    r.expected_return_pct,
    dm.daily_return_pct,
    ROUND((r.expected_return_pct - dm.daily_return_pct)::NUMERIC, 6) AS difference
FROM recomputed r
JOIN daily_metrics dm ON dm.asset_id = r.asset_id AND dm.date = r.date
JOIN assets a ON a.asset_id = r.asset_id
WHERE r.expected_return_pct IS NOT NULL
  AND dm.daily_return_pct IS NOT NULL
  AND ABS(r.expected_return_pct - dm.daily_return_pct) > 0.001
ORDER BY ABS(r.expected_return_pct - dm.daily_return_pct) DESC
LIMIT 20;

-- EXPECTED: 0 rows. A tolerance of 0.001 percentage points accounts for
-- rounding differences between Python and PostgreSQL arithmetic. Any rows
-- returned indicate a meaningful discrepancy between the stored metric
-- and the expected value computed directly from prices.


-- ############################################################################
-- END OF DATA INTEGRITY CHECKS
-- ############################################################################
-- Summary of checks:
--
-- Section 1 (Row Count Alignment):
--   1.1  Total row counts match
--   1.2  Per-asset row counts match
--   1.3  No orphaned daily_prices rows
--   1.4  No orphaned daily_metrics rows
--
-- Section 2 (NULL Waterfall):
--   2.1  Aggregate NULL counts per asset (informational)
--   2.2  Flag assets with unexpected NULL counts (pass = 0 rows)
--   2.3  NULLs are in earliest rows, not scattered (pass = 0 rows)
--
-- Section 3 (Value Range Sanity):
--   3.1  daily_return_pct in [-100, +500]
--   3.2  daily_range_pct >= 0
--   3.3  vol_7d >= 0 and vol_30d >= 0
--   3.4  sma_7 > 0 and sma_30 > 0
--   3.5  volume_ratio_30d >= 0
--   3.6  Summary statistics (informational, manual review)
--
-- Section 4 (Cross-Metric Consistency):
--   4.1  vol_7d variability > vol_30d variability
--   4.2  SMA bullish alignment during BTC bull run
--   4.3  Volume ratio spike on FTX collapse date
--   4.4  vol_7d and vol_30d long-run means comparable
--   4.5  SMA values bounded by min/max close in window
--
-- Section 5 (Known Event Spot Checks):
--   5.1  BTC return spot checks on 4 known dates
--   5.2  BTC vol_30d regime (FTX vs accumulation)
--   5.3  Multi-asset return + volume on FTX collapse
--   5.4  BTC daily_range_pct during FTX week
--   5.5  Return sign matches price direction
--   5.6  Return magnitude via direct recomputation
--
-- All "pass = 0 rows" checks should return empty result sets.
-- All informational checks should be reviewed manually for plausibility.
-- ############################################################################
