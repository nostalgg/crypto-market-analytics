-- ============================================================================
-- Q4: Drawdown Analysis — Max Drawdown per Asset
-- File: 04_drawdown_analysis.sql
-- Business Question: What is the maximum peak-to-trough decline for each
--                    asset across defined time windows (30d, 90d, 1y, all)?
--                    When did drawdowns start, hit bottom, and recover?
-- SQL Features: Cumulative MAX via window function, percentage decline
--               from running peak, date range filtering
-- ============================================================================
--
-- TECHNIQUE: Running Maximum + Drawdown Percentage
-- =================================================
-- Drawdown at time t = (price_t - peak_t) / peak_t
-- Where peak_t = MAX(close) from the start of the period up to time t.
--
-- The maximum drawdown is the worst (most negative) drawdown value in the
-- period. It represents the largest loss an investor would have suffered
-- buying at the peak and holding through the trough.
--
-- This is a standard risk metric used in hedge fund reporting, portfolio
-- analysis, and regulatory filings (e.g., AIFMD).
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 4A: Running drawdown time series — all assets, full period
-- ---------------------------------------------------------------------------
-- Computes the drawdown at every point in time for each asset, using
-- the running maximum from the beginning of the dataset.

WITH running_peak AS (
    SELECT
        dp.asset_id,
        a.symbol,
        dp.date,
        dp.close,
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_close
    FROM daily_prices dp
    JOIN assets a ON a.asset_id = dp.asset_id
)

SELECT
    symbol,
    date,
    close,
    peak_close,
    ROUND(
        ((close - peak_close) / peak_close * 100)::NUMERIC,
        2
    ) AS drawdown_pct
FROM running_peak
ORDER BY asset_id, date;

-- INTERPRETATION:
--   drawdown_pct is always <= 0 (0 means at a new high).
--   The most negative values represent the deepest drawdowns.


-- ---------------------------------------------------------------------------
-- QUERY 4B: Maximum drawdown per asset — full period
-- ---------------------------------------------------------------------------
-- The single worst peak-to-trough decline for each asset across the
-- entire dataset, with the date it occurred.

WITH running_peak AS (
    SELECT
        dp.asset_id,
        a.symbol,
        dp.date,
        dp.close,
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_close
    FROM daily_prices dp
    JOIN assets a ON a.asset_id = dp.asset_id
),

drawdowns AS (
    SELECT
        asset_id,
        symbol,
        date,
        close,
        peak_close,
        ROUND(
            ((close - peak_close) / peak_close * 100)::NUMERIC,
            2
        ) AS drawdown_pct
    FROM running_peak
),

ranked AS (
    SELECT
        symbol,
        date AS trough_date,
        close AS trough_price,
        peak_close AS peak_price,
        drawdown_pct,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id
            ORDER BY drawdown_pct ASC  -- most negative first
        ) AS rn
    FROM drawdowns
)

SELECT
    symbol,
    drawdown_pct AS max_drawdown_pct,
    peak_price,
    trough_price,
    trough_date
FROM ranked
WHERE rn = 1
ORDER BY drawdown_pct ASC;

-- EXPECTED:
--   SOL likely has the deepest drawdown (FTX-linked, crashed from ~$260
--   to ~$8 in 2022). LINK and DOT also suffered severe drawdowns.
--   BTC's max drawdown is typically 70-80% in bear markets.


-- ---------------------------------------------------------------------------
-- QUERY 4C: Maximum drawdown by rolling time window (30d, 90d, 1y)
-- ---------------------------------------------------------------------------
-- For each asset, computes the max drawdown within rolling 30-day,
-- 90-day, and 1-year windows. This shows the worst short/medium/long-term
-- drawdown experienced.

WITH windowed_drawdowns AS (
    SELECT
        dp.asset_id,
        a.symbol,
        dp.date,
        dp.close,
        -- 30-day rolling peak
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS peak_30d,
        -- 90-day rolling peak
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS peak_90d,
        -- 365-day rolling peak
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS peak_1y
    FROM daily_prices dp
    JOIN assets a ON a.asset_id = dp.asset_id
)

SELECT
    symbol,
    -- Max drawdown within any 30-day window
    ROUND(
        MIN((close - peak_30d) / peak_30d * 100)::NUMERIC,
        2
    ) AS max_dd_30d,
    -- Max drawdown within any 90-day window
    ROUND(
        MIN((close - peak_90d) / peak_90d * 100)::NUMERIC,
        2
    ) AS max_dd_90d,
    -- Max drawdown within any 1-year window
    ROUND(
        MIN((close - peak_1y) / peak_1y * 100)::NUMERIC,
        2
    ) AS max_dd_1y
FROM windowed_drawdowns
GROUP BY asset_id, symbol
ORDER BY max_dd_30d ASC;

-- INTERPRETATION:
--   30d max drawdown shows the worst short-term crash.
--   90d shows medium-term damage.
--   1y shows the worst annual drawdown (bear market depth).
--   Altcoins typically show worse drawdowns than BTC across all windows.


-- ---------------------------------------------------------------------------
-- QUERY 4D: Drawdown durations — how long did it take to recover?
-- ---------------------------------------------------------------------------
-- For each new all-time-high (ATH), compute how many days it took to
-- reach the next ATH. This is the "time underwater" or recovery duration.

WITH running_peak AS (
    SELECT
        dp.asset_id,
        a.symbol,
        dp.date,
        dp.close,
        MAX(dp.close) OVER (
            PARTITION BY dp.asset_id
            ORDER BY dp.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_close
    FROM daily_prices dp
    JOIN assets a ON a.asset_id = dp.asset_id
),

ath_flags AS (
    -- Flag days where a new ATH is reached (close = running peak)
    SELECT
        asset_id,
        symbol,
        date,
        close,
        peak_close,
        CASE WHEN close >= peak_close THEN 1 ELSE 0 END AS is_ath
    FROM running_peak
),

ath_groups AS (
    -- Create groups: each ATH starts a new group, the period below ATH
    -- between two ATH dates is one "underwater" period.
    SELECT
        asset_id,
        symbol,
        date,
        close,
        peak_close,
        is_ath,
        SUM(is_ath) OVER (
            PARTITION BY asset_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ath_group
    FROM ath_flags
)

SELECT
    symbol,
    ath_group,
    MIN(date) AS period_start,
    MAX(date) AS period_end,
    (MAX(date) - MIN(date)) AS days_underwater,
    ROUND(
        MIN((close - peak_close) / peak_close * 100)::NUMERIC,
        2
    ) AS max_dd_in_period
FROM ath_groups
WHERE is_ath = 0  -- only underwater periods
GROUP BY asset_id, symbol, ath_group
HAVING (MAX(date) - MIN(date)) >= 7  -- filter out trivial 1-2 day dips
ORDER BY symbol, period_start;

-- INTERPRETATION:
--   Long underwater periods (200+ days) correspond to bear markets.
--   The 2022 bear market should show extended underwater periods for all
--   assets, with recovery happening in 2023-2024 for most.
