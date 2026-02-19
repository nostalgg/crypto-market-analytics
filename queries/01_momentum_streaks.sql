-- ============================================================================
-- Q1: Momentum & Reversal — Consecutive Up-Day Streaks
-- File: 01_momentum_streaks.sql
-- Business Question: Which assets had the strongest consecutive up-day
--                    streaks in the dataset? Identify the top streaks by
--                    length and cumulative gain.
-- SQL Features: LAG(), conditional running count (streak detection),
--               window functions, CTEs
-- ============================================================================
--
-- TECHNIQUE: Streak Detection via "Island" Pattern
-- =================================================
-- This is a classic gaps-and-islands problem. We detect consecutive up-days
-- by:
--   1. Flagging each day as "up" (return > 0) or "not up"
--   2. Using a running count of non-up days to create group IDs
--   3. Consecutive up-days share the same group ID (the count of non-up days
--      before them doesn't change)
--   4. Aggregating within each group gives streak length and cumulative return
--
-- This pattern is widely used in financial analytics for momentum detection,
-- win/loss streaks, and regime persistence analysis.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1A: Top 20 longest consecutive up-day streaks across all assets
-- ---------------------------------------------------------------------------

WITH up_flags AS (
    -- Step 1: Flag each day as up (1) or not (0).
    -- An "up day" is defined as close > previous close (daily_return_pct > 0).
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date,
        dm.daily_return_pct,
        dp.close,
        CASE
            WHEN dm.daily_return_pct > 0 THEN 1
            ELSE 0
        END AS is_up
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    JOIN daily_prices dp ON dp.asset_id = dm.asset_id AND dp.date = dm.date
    WHERE dm.daily_return_pct IS NOT NULL
),

streak_groups AS (
    -- Step 2: Create streak group IDs using the "island" technique.
    -- SUM of non-up days creates a monotonically increasing counter that
    -- stays constant during consecutive up-day sequences.
    SELECT
        asset_id,
        symbol,
        date,
        daily_return_pct,
        close,
        is_up,
        SUM(CASE WHEN is_up = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY asset_id
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak_group
    FROM up_flags
),

streaks AS (
    -- Step 3: Aggregate each streak — length, dates, cumulative return.
    -- Only keep up-day streaks (is_up = 1).
    SELECT
        asset_id,
        symbol,
        streak_group,
        COUNT(*)                                    AS streak_length,
        MIN(date)                                   AS streak_start,
        MAX(date)                                   AS streak_end,
        ROUND(SUM(daily_return_pct)::NUMERIC, 2)    AS cumulative_return_pct,
        ROUND(MIN(daily_return_pct)::NUMERIC, 2)    AS min_daily_return,
        ROUND(MAX(daily_return_pct)::NUMERIC, 2)    AS max_daily_return
    FROM streak_groups
    WHERE is_up = 1
    GROUP BY asset_id, symbol, streak_group
)

SELECT
    RANK() OVER (ORDER BY streak_length DESC, cumulative_return_pct DESC) AS rank,
    symbol,
    streak_length,
    streak_start,
    streak_end,
    cumulative_return_pct,
    min_daily_return,
    max_daily_return
FROM streaks
ORDER BY streak_length DESC, cumulative_return_pct DESC
LIMIT 20;

-- EXPECTED OUTPUT COLUMNS:
--   rank | symbol | streak_length | streak_start | streak_end |
--   cumulative_return_pct | min_daily_return | max_daily_return
--
-- INTERPRETATION:
--   Longer streaks with higher cumulative returns indicate strong momentum.
--   Streaks during known bull periods (e.g., post-ETF approval Jan 2024,
--   post-election Nov 2024) validate the data.


-- ---------------------------------------------------------------------------
-- QUERY 1B: Top 5 streaks per asset (shows each asset's best momentum run)
-- ---------------------------------------------------------------------------

WITH up_flags AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date,
        dm.daily_return_pct,
        CASE WHEN dm.daily_return_pct > 0 THEN 1 ELSE 0 END AS is_up
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE dm.daily_return_pct IS NOT NULL
),

streak_groups AS (
    SELECT
        asset_id, symbol, date, daily_return_pct, is_up,
        SUM(CASE WHEN is_up = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY asset_id ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak_group
    FROM up_flags
),

streaks AS (
    SELECT
        asset_id,
        symbol,
        COUNT(*)                                     AS streak_length,
        MIN(date)                                    AS streak_start,
        MAX(date)                                    AS streak_end,
        ROUND(SUM(daily_return_pct)::NUMERIC, 2)     AS cumulative_return_pct
    FROM streak_groups
    WHERE is_up = 1
    GROUP BY asset_id, symbol, streak_group
),

ranked AS (
    SELECT
        symbol,
        streak_length,
        streak_start,
        streak_end,
        cumulative_return_pct,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY streak_length DESC, cumulative_return_pct DESC
        ) AS asset_rank
    FROM streaks
)

SELECT
    symbol,
    asset_rank,
    streak_length,
    streak_start,
    streak_end,
    cumulative_return_pct
FROM ranked
WHERE asset_rank <= 5
ORDER BY symbol, asset_rank;


-- ---------------------------------------------------------------------------
-- QUERY 1C: Monthly streak frequency — which months have the most momentum?
-- ---------------------------------------------------------------------------
-- Counts how many up-streaks of 3+ days started in each calendar month.
-- Useful for seasonal momentum analysis.

WITH up_flags AS (
    SELECT
        dm.asset_id,
        dm.date,
        CASE WHEN dm.daily_return_pct > 0 THEN 1 ELSE 0 END AS is_up
    FROM daily_metrics dm
    WHERE dm.daily_return_pct IS NOT NULL
),

streak_groups AS (
    SELECT
        asset_id, date, is_up,
        SUM(CASE WHEN is_up = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY asset_id ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS streak_group
    FROM up_flags
),

streaks AS (
    SELECT
        asset_id,
        MIN(date) AS streak_start,
        COUNT(*)  AS streak_length
    FROM streak_groups
    WHERE is_up = 1
    GROUP BY asset_id, streak_group
    HAVING COUNT(*) >= 3
)

SELECT
    dd.month,
    COUNT(*)                                AS streak_count,
    ROUND(AVG(s.streak_length)::NUMERIC, 1) AS avg_streak_length,
    MAX(s.streak_length)                    AS max_streak_length
FROM streaks s
JOIN date_dim dd ON dd.date_id = s.streak_start
GROUP BY dd.month
ORDER BY streak_count DESC;
