-- ============================================================================
-- Q3: Correlation Analysis — BTC/ETH Co-Movement & Decorrelation
-- File: 03_correlation_analysis.sql
-- Business Question: Do BTC and ETH move together? In which periods do
--                    they decorrelate? How do other assets correlate with
--                    BTC across different time windows?
-- SQL Features: CORR() aggregate function, SELF-JOIN for pair construction,
--               rolling correlation via subquery/LATERAL, FILTER clause
-- ============================================================================
--
-- TECHNIQUE: Pearson Correlation on Returns
-- ==========================================
-- Correlation is computed on daily_return_pct, NOT on raw prices.
-- Raw prices are non-stationary (trending upward/downward), which produces
-- spuriously high correlations. Returns are (approximately) stationary,
-- making Pearson correlation meaningful.
--
-- CORR(x, y) in PostgreSQL computes the Pearson correlation coefficient,
-- returning a value between -1.0 and +1.0.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 3A: Full-period correlation matrix — all asset pairs
-- ---------------------------------------------------------------------------
-- Computes the Pearson correlation of daily returns between every pair of
-- assets over the entire dataset. This gives the "structural" co-movement.

SELECT
    a1.symbol AS asset_1,
    a2.symbol AS asset_2,
    COUNT(*) AS overlapping_days,
    ROUND(CORR(dm1.daily_return_pct, dm2.daily_return_pct)::NUMERIC, 4)
        AS correlation
FROM daily_metrics dm1
JOIN daily_metrics dm2
    ON dm1.date = dm2.date
    AND dm1.asset_id < dm2.asset_id  -- avoid duplicates and self-pairs
JOIN assets a1 ON a1.asset_id = dm1.asset_id
JOIN assets a2 ON a2.asset_id = dm2.asset_id
WHERE dm1.daily_return_pct IS NOT NULL
  AND dm2.daily_return_pct IS NOT NULL
GROUP BY a1.symbol, a2.symbol
ORDER BY correlation DESC;

-- INTERPRETATION:
--   BTC-ETH correlation is typically 0.75-0.90 over the full period.
--   Layer-1 assets (SOL, ADA, AVAX) tend to correlate 0.60-0.85 with BTC.
--   LINK (oracle) may show slightly lower correlation with pure Layer-1s.


-- ---------------------------------------------------------------------------
-- QUERY 3B: Rolling 30-day correlation — BTC vs ETH over time
-- ---------------------------------------------------------------------------
-- Shows how the BTC-ETH correlation evolves. Decorrelation periods often
-- correspond to ETH-specific events (Merge, Shanghai upgrade) or
-- BTC-specific events (halving, ETF approval).

SELECT
    dm_btc.date,
    CORR(dm_btc_w.daily_return_pct, dm_eth_w.daily_return_pct)::NUMERIC
        AS rolling_corr_30d
FROM daily_metrics dm_btc
JOIN assets a_btc ON a_btc.asset_id = dm_btc.asset_id AND a_btc.symbol = 'BTC'
-- Self-join to create the 30-day window for BTC
JOIN LATERAL (
    SELECT date, daily_return_pct
    FROM daily_metrics
    WHERE asset_id = dm_btc.asset_id
      AND date BETWEEN dm_btc.date - INTERVAL '29 days' AND dm_btc.date
      AND daily_return_pct IS NOT NULL
) dm_btc_w ON TRUE
-- Match with ETH returns on the same dates within the window
JOIN daily_metrics dm_eth_w
    ON dm_eth_w.date = dm_btc_w.date
JOIN assets a_eth ON a_eth.asset_id = dm_eth_w.asset_id AND a_eth.symbol = 'ETH'
WHERE dm_btc.daily_return_pct IS NOT NULL
  AND dm_eth_w.daily_return_pct IS NOT NULL
GROUP BY dm_btc.date
HAVING COUNT(*) >= 20  -- require at least 20 overlapping days in window
ORDER BY dm_btc.date;

-- INTERPRETATION:
--   Look for periods where rolling correlation drops below 0.5 — these are
--   decorrelation events. Common triggers:
--     - ETH Merge (Sep 2022): ETH had unique dynamics
--     - BTC ETF approval (Jan 2024): BTC-specific demand
--     - Alt-season rallies: altcoins rally while BTC consolidates


-- ---------------------------------------------------------------------------
-- QUERY 3C: Quarterly correlation — BTC vs all assets by quarter
-- ---------------------------------------------------------------------------
-- Shows how each asset's correlation with BTC changes across calendar
-- quarters. Useful for identifying seasonal patterns or regime shifts.

SELECT
    dd.year,
    dd.quarter,
    a2.symbol AS asset,
    COUNT(*) AS days,
    ROUND(
        CORR(dm_btc.daily_return_pct, dm_other.daily_return_pct)::NUMERIC,
        4
    ) AS corr_with_btc
FROM daily_metrics dm_btc
JOIN assets a_btc ON a_btc.asset_id = dm_btc.asset_id AND a_btc.symbol = 'BTC'
JOIN daily_metrics dm_other
    ON dm_other.date = dm_btc.date
    AND dm_other.asset_id != dm_btc.asset_id
JOIN assets a2 ON a2.asset_id = dm_other.asset_id
JOIN date_dim dd ON dd.date_id = dm_btc.date
WHERE dm_btc.daily_return_pct IS NOT NULL
  AND dm_other.daily_return_pct IS NOT NULL
GROUP BY dd.year, dd.quarter, a2.symbol
HAVING COUNT(*) >= 30  -- at least 30 days for meaningful correlation
ORDER BY dd.year, dd.quarter, a2.symbol;


-- ---------------------------------------------------------------------------
-- QUERY 3D: Correlation during stress vs calm periods
-- ---------------------------------------------------------------------------
-- Financial theory: correlations tend to increase during market stress
-- ("correlations go to 1 in a crisis"). This query compares correlations
-- during high-vol regimes vs low-vol regimes for BTC.

WITH btc_regime AS (
    SELECT
        dm.date,
        CASE
            WHEN dm.vol_30d >= 4.0 THEN 'STRESS'
            ELSE 'CALM'
        END AS market_regime
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE a.symbol = 'BTC'
      AND dm.vol_30d IS NOT NULL
)

SELECT
    br.market_regime,
    a2.symbol AS asset,
    COUNT(*) AS days,
    ROUND(
        CORR(dm_btc.daily_return_pct, dm_other.daily_return_pct)::NUMERIC,
        4
    ) AS corr_with_btc
FROM btc_regime br
JOIN daily_metrics dm_btc
    ON dm_btc.date = br.date
JOIN assets a_btc ON a_btc.asset_id = dm_btc.asset_id AND a_btc.symbol = 'BTC'
JOIN daily_metrics dm_other
    ON dm_other.date = br.date
    AND dm_other.asset_id != dm_btc.asset_id
JOIN assets a2 ON a2.asset_id = dm_other.asset_id
WHERE dm_btc.daily_return_pct IS NOT NULL
  AND dm_other.daily_return_pct IS NOT NULL
GROUP BY br.market_regime, a2.symbol
HAVING COUNT(*) >= 20
ORDER BY a2.symbol, br.market_regime;

-- EXPECTED: Correlation during STRESS should be higher than during CALM
-- for most asset pairs. This confirms the "correlation → 1 in panic" effect.
