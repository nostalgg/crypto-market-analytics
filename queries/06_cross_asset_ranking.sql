-- ============================================================================
-- Q6: Cross-Asset Ranking — Weekly Performance Rankings
-- File: 06_cross_asset_ranking.sql
-- Business Question: How do assets rank against each other each week?
--                    Which assets consistently outperform or underperform?
--                    Can we identify "alt-season" vs "BTC-dominance" regimes?
-- SQL Features: NTILE(), RANK(), DENSE_RANK(), ROW_NUMBER(),
--               date_trunc for weekly aggregation, GROUPING SETS
-- ============================================================================
--
-- TECHNIQUE: Partitioned Ranking over Weekly Returns
-- ====================================================
-- Weekly returns are computed by aggregating daily returns within each
-- ISO week. Then assets are ranked within each week using multiple ranking
-- functions to demonstrate proficiency with each.
--
-- NTILE(4) divides assets into quartiles (top 25%, upper-mid, lower-mid,
-- bottom 25%) — useful for quartile-based factor analysis.
-- RANK() and DENSE_RANK() handle ties differently — both are shown.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 6A: Weekly performance with full ranking suite
-- ---------------------------------------------------------------------------
-- Computes weekly returns per asset, then ranks assets within each week
-- using RANK, DENSE_RANK, NTILE(4), and ROW_NUMBER.

WITH weekly_returns AS (
    -- Aggregate daily returns into weekly returns.
    -- Weekly return ≈ sum of daily returns (approximation; exact would require
    -- compounding, but sum is standard for short periods and simpler to explain).
    SELECT
        dm.asset_id,
        a.symbol,
        dd.year,
        dd.week AS iso_week,
        -- Use Monday of the week as the week identifier
        MIN(dm.date) AS week_start,
        MAX(dm.date) AS week_end,
        COUNT(*) AS trading_days,
        ROUND(SUM(dm.daily_return_pct)::NUMERIC, 2) AS weekly_return_pct,
        ROUND(AVG(dm.vol_7d)::NUMERIC, 4)           AS avg_vol_7d
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    JOIN date_dim dd ON dd.date_id = dm.date
    WHERE dm.daily_return_pct IS NOT NULL
    GROUP BY dm.asset_id, a.symbol, dd.year, dd.week
    HAVING COUNT(*) >= 3  -- require at least 3 trading days for a valid week
)

SELECT
    year,
    iso_week,
    week_start,
    week_end,
    symbol,
    weekly_return_pct,

    -- RANK: ties get the same rank, then skip (1,1,3,4...)
    RANK() OVER (
        PARTITION BY year, iso_week
        ORDER BY weekly_return_pct DESC
    ) AS rank_by_return,

    -- DENSE_RANK: ties get the same rank, no skip (1,1,2,3...)
    DENSE_RANK() OVER (
        PARTITION BY year, iso_week
        ORDER BY weekly_return_pct DESC
    ) AS dense_rank_by_return,

    -- NTILE(4): divide into quartiles (1=top 25%, 4=bottom 25%)
    NTILE(4) OVER (
        PARTITION BY year, iso_week
        ORDER BY weekly_return_pct DESC
    ) AS performance_quartile,

    -- ROW_NUMBER: unique ordering, breaks ties arbitrarily
    ROW_NUMBER() OVER (
        PARTITION BY year, iso_week
        ORDER BY weekly_return_pct DESC
    ) AS row_num

FROM weekly_returns
ORDER BY year, iso_week, rank_by_return;


-- ---------------------------------------------------------------------------
-- QUERY 6B: Asset performance quartile distribution — "consistency score"
-- ---------------------------------------------------------------------------
-- For each asset, how often does it land in each performance quartile?
-- An asset that frequently appears in Q1 is a consistent outperformer.

WITH weekly_returns AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dd.year,
        dd.week AS iso_week,
        SUM(dm.daily_return_pct) AS weekly_return_pct
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    JOIN date_dim dd ON dd.date_id = dm.date
    WHERE dm.daily_return_pct IS NOT NULL
    GROUP BY dm.asset_id, a.symbol, dd.year, dd.week
    HAVING COUNT(*) >= 3
),

quartiled AS (
    SELECT
        symbol,
        year,
        iso_week,
        weekly_return_pct,
        NTILE(4) OVER (
            PARTITION BY year, iso_week
            ORDER BY weekly_return_pct DESC
        ) AS quartile
    FROM weekly_returns
)

SELECT
    symbol,
    COUNT(*) FILTER (WHERE quartile = 1) AS weeks_in_q1,
    COUNT(*) FILTER (WHERE quartile = 2) AS weeks_in_q2,
    COUNT(*) FILTER (WHERE quartile = 3) AS weeks_in_q3,
    COUNT(*) FILTER (WHERE quartile = 4) AS weeks_in_q4,
    COUNT(*) AS total_weeks,
    ROUND(
        COUNT(*) FILTER (WHERE quartile = 1)::NUMERIC / COUNT(*) * 100,
        1
    ) AS pct_top_quartile,
    ROUND(
        COUNT(*) FILTER (WHERE quartile = 4)::NUMERIC / COUNT(*) * 100,
        1
    ) AS pct_bottom_quartile
FROM quartiled
GROUP BY symbol
ORDER BY pct_top_quartile DESC;

-- INTERPRETATION:
--   An even distribution (~25% each) means no consistent edge.
--   SOL and BNB may show more Q1 appearances during the 2023-2024 bull run.
--   DOT likely appears more in Q4 (declining narrative, weaker performance).


-- ---------------------------------------------------------------------------
-- QUERY 6C: Yearly ranking summary — ROLLUP for subtotals
-- ---------------------------------------------------------------------------
-- Shows each asset's average weekly rank by year, with a grand total.
-- Uses GROUPING SETS to produce both per-year and overall summaries.

WITH weekly_returns AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dd.year,
        dd.week AS iso_week,
        SUM(dm.daily_return_pct) AS weekly_return_pct
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    JOIN date_dim dd ON dd.date_id = dm.date
    WHERE dm.daily_return_pct IS NOT NULL
    GROUP BY dm.asset_id, a.symbol, dd.year, dd.week
    HAVING COUNT(*) >= 3
),

ranked AS (
    SELECT
        symbol,
        year,
        iso_week,
        weekly_return_pct,
        RANK() OVER (
            PARTITION BY year, iso_week
            ORDER BY weekly_return_pct DESC
        ) AS weekly_rank
    FROM weekly_returns
)

SELECT
    COALESCE(symbol, 'ALL ASSETS') AS symbol,
    COALESCE(year::TEXT, 'ALL YEARS') AS year,
    COUNT(*) AS weeks,
    ROUND(AVG(weekly_rank)::NUMERIC, 2) AS avg_rank,
    ROUND(AVG(weekly_return_pct)::NUMERIC, 2) AS avg_weekly_return,
    MIN(weekly_rank) AS best_rank,
    MAX(weekly_rank) AS worst_rank
FROM ranked
GROUP BY ROLLUP (symbol, year)
ORDER BY symbol NULLS LAST, year NULLS LAST;


-- ---------------------------------------------------------------------------
-- QUERY 6D: BTC dominance vs alt-season detection
-- ---------------------------------------------------------------------------
-- When BTC ranks #1 among the 8 assets, it's "BTC dominance."
-- When BTC ranks below median, altcoins are outperforming ("alt-season").
-- This query identifies and counts these regimes by month.

WITH weekly_returns AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dd.year,
        dd.month,
        dd.week AS iso_week,
        SUM(dm.daily_return_pct) AS weekly_return_pct
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    JOIN date_dim dd ON dd.date_id = dm.date
    WHERE dm.daily_return_pct IS NOT NULL
    GROUP BY dm.asset_id, a.symbol, dd.year, dd.month, dd.week
    HAVING COUNT(*) >= 3
),

btc_ranked AS (
    SELECT
        year,
        month,
        iso_week,
        weekly_return_pct,
        RANK() OVER (
            PARTITION BY year, iso_week
            ORDER BY weekly_return_pct DESC
        ) AS weekly_rank
    FROM weekly_returns
    WHERE symbol = 'BTC'
)

SELECT
    year,
    month,
    COUNT(*) AS total_weeks,
    COUNT(*) FILTER (WHERE weekly_rank = 1) AS btc_rank_1_weeks,
    COUNT(*) FILTER (WHERE weekly_rank <= 3) AS btc_top_3_weeks,
    COUNT(*) FILTER (WHERE weekly_rank >= 5) AS btc_bottom_half_weeks,
    CASE
        WHEN COUNT(*) FILTER (WHERE weekly_rank <= 2)::NUMERIC / COUNT(*) > 0.5
            THEN 'BTC_DOMINANCE'
        WHEN COUNT(*) FILTER (WHERE weekly_rank >= 5)::NUMERIC / COUNT(*) > 0.5
            THEN 'ALT_SEASON'
        ELSE 'MIXED'
    END AS regime
FROM btc_ranked
GROUP BY year, month
ORDER BY year, month;

-- INTERPRETATION:
--   BTC_DOMINANCE periods often align with institutional inflows (ETF era).
--   ALT_SEASON periods align with risk-on sentiment and speculative cycles.
--   This is a simplified version of the "BTC dominance index" analysis
--   commonly used in crypto market commentary.
