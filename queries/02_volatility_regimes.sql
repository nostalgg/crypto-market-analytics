-- ============================================================================
-- Q2: Volatility Regimes — Identify High/Low Volatility Periods
-- File: 02_volatility_regimes.sql
-- Business Question: Can we identify distinct volatility regimes (low,
--                    medium, high, extreme) for each asset? How long does
--                    each regime last, and do regimes cluster around
--                    known market events?
-- SQL Features: CASE-based classification, window functions, LAG for
--               regime transition detection, date arithmetic
-- ============================================================================
--
-- TECHNIQUE: Regime Classification + Transition Detection
-- ========================================================
-- We classify each day into a volatility regime based on vol_30d thresholds,
-- then detect regime transitions (when the classification changes day-to-day)
-- to identify contiguous regime periods and their durations.
--
-- Thresholds (from Finance Expert validation, crypto-specific):
--   - LOW:     vol_30d < 2.0 pp  (annualized ~ < 38%)
--   - MEDIUM:  vol_30d 2.0-4.0   (annualized ~ 38-76%)
--   - HIGH:    vol_30d 4.0-6.0   (annualized ~ 76-115%)
--   - EXTREME: vol_30d > 6.0     (annualized ~ > 115%)
--
-- Note: vol_30d is stored as daily (not annualized) percentage points.
-- To annualize: vol_30d * sqrt(365). Example: 3.0 pp daily → 57.3% annual.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 2A: Daily regime classification for all assets
-- ---------------------------------------------------------------------------

WITH regime_classified AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date,
        ROUND(dm.vol_30d::NUMERIC, 4) AS vol_30d,
        ROUND((dm.vol_30d * SQRT(365))::NUMERIC, 2) AS vol_30d_annualized,
        CASE
            WHEN dm.vol_30d < 2.0  THEN 'LOW'
            WHEN dm.vol_30d < 4.0  THEN 'MEDIUM'
            WHEN dm.vol_30d < 6.0  THEN 'HIGH'
            ELSE                        'EXTREME'
        END AS vol_regime
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE dm.vol_30d IS NOT NULL
)

SELECT *
FROM regime_classified
ORDER BY asset_id, date;


-- ---------------------------------------------------------------------------
-- QUERY 2B: Regime periods — contiguous blocks with duration
-- ---------------------------------------------------------------------------
-- Uses the gaps-and-islands technique to identify contiguous periods where
-- an asset stays in the same volatility regime. Reports start/end dates,
-- duration, and average volatility within each period.

WITH regime_classified AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date,
        dm.vol_30d,
        CASE
            WHEN dm.vol_30d < 2.0  THEN 'LOW'
            WHEN dm.vol_30d < 4.0  THEN 'MEDIUM'
            WHEN dm.vol_30d < 6.0  THEN 'HIGH'
            ELSE                        'EXTREME'
        END AS vol_regime
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE dm.vol_30d IS NOT NULL
),

regime_changes AS (
    -- Flag rows where the regime changes from the previous day.
    SELECT
        asset_id,
        symbol,
        date,
        vol_30d,
        vol_regime,
        CASE
            WHEN vol_regime != LAG(vol_regime) OVER (
                PARTITION BY asset_id ORDER BY date
            ) THEN 1
            WHEN LAG(vol_regime) OVER (
                PARTITION BY asset_id ORDER BY date
            ) IS NULL THEN 1
            ELSE 0
        END AS is_new_regime
    FROM regime_classified
),

regime_groups AS (
    -- Assign group IDs: cumulative sum of regime-change flags.
    SELECT
        asset_id,
        symbol,
        date,
        vol_30d,
        vol_regime,
        SUM(is_new_regime) OVER (
            PARTITION BY asset_id ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS regime_group
    FROM regime_changes
)

SELECT
    symbol,
    vol_regime,
    MIN(date)                                       AS regime_start,
    MAX(date)                                       AS regime_end,
    (MAX(date) - MIN(date) + 1)                     AS duration_days,
    ROUND(AVG(vol_30d)::NUMERIC, 4)                 AS avg_vol_30d,
    ROUND((AVG(vol_30d) * SQRT(365))::NUMERIC, 2)   AS avg_vol_annualized,
    ROUND(MIN(vol_30d)::NUMERIC, 4)                 AS min_vol_30d,
    ROUND(MAX(vol_30d)::NUMERIC, 4)                 AS max_vol_30d
FROM regime_groups
GROUP BY asset_id, symbol, vol_regime, regime_group
ORDER BY symbol, MIN(date);


-- ---------------------------------------------------------------------------
-- QUERY 2C: Regime distribution summary — % of time in each regime per asset
-- ---------------------------------------------------------------------------

WITH regime_classified AS (
    SELECT
        dm.asset_id,
        a.symbol,
        CASE
            WHEN dm.vol_30d < 2.0  THEN 'LOW'
            WHEN dm.vol_30d < 4.0  THEN 'MEDIUM'
            WHEN dm.vol_30d < 6.0  THEN 'HIGH'
            ELSE                        'EXTREME'
        END AS vol_regime
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE dm.vol_30d IS NOT NULL
)

SELECT
    symbol,
    vol_regime,
    COUNT(*) AS days_in_regime,
    ROUND(
        COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY symbol) * 100,
        1
    ) AS pct_of_total
FROM regime_classified
GROUP BY symbol, vol_regime
ORDER BY symbol,
    CASE vol_regime
        WHEN 'LOW'     THEN 1
        WHEN 'MEDIUM'  THEN 2
        WHEN 'HIGH'    THEN 3
        WHEN 'EXTREME' THEN 4
    END;

-- INTERPRETATION:
--   BTC typically spends more time in LOW/MEDIUM regimes (it is the most
--   "mature" crypto). Altcoins like SOL, AVAX spend more time in HIGH/EXTREME.
--   EXTREME regimes should cluster around known crash dates (FTX Nov 2022,
--   Terra May 2022).


-- ---------------------------------------------------------------------------
-- QUERY 2D: Regime transitions near market events
-- ---------------------------------------------------------------------------
-- For each market event, show the volatility regime of BTC in the 7 days
-- before and after the event. Useful for seeing how events trigger regime
-- shifts.

SELECT
    me.event_date,
    me.title,
    me.event_type,
    dm.date AS observation_date,
    (dm.date - me.event_date) AS days_from_event,
    ROUND(dm.vol_30d::NUMERIC, 4) AS vol_30d,
    CASE
        WHEN dm.vol_30d < 2.0  THEN 'LOW'
        WHEN dm.vol_30d < 4.0  THEN 'MEDIUM'
        WHEN dm.vol_30d < 6.0  THEN 'HIGH'
        ELSE                        'EXTREME'
    END AS vol_regime
FROM market_events me
CROSS JOIN LATERAL (
    SELECT dm2.date, dm2.vol_30d
    FROM daily_metrics dm2
    JOIN assets a ON a.asset_id = dm2.asset_id
    WHERE a.symbol = 'BTC'
      AND dm2.date BETWEEN me.event_date - INTERVAL '7 days'
                        AND me.event_date + INTERVAL '7 days'
      AND dm2.vol_30d IS NOT NULL
) dm
ORDER BY me.event_date, dm.date;
