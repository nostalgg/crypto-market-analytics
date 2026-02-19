-- ============================================================================
-- Q5: Volume Anomalies — Days with Unusually High Trading Volume
-- File: 05_volume_anomalies.sql
-- Business Question: Which days had trading volume > 2x the 30-day average?
--                    Do these spikes correspond to known market events or
--                    potential breakout/breakdown signals?
-- SQL Features: Pre-computed volume_ratio_30d, FILTER clause, JOIN with
--               market_events, window functions for context
-- ============================================================================
--
-- TECHNIQUE: Threshold-Based Anomaly Detection
-- ==============================================
-- volume_ratio_30d is pre-computed in daily_metrics:
--   volume_ratio_30d = today's volume / avg(prior 30 days' volume)
--
-- A ratio > 2.0 means today's volume is more than double the recent average,
-- which typically signals: institutional activity, panic selling/buying,
-- news-driven events, or technical breakouts.
--
-- We join with market_events to see which spikes have known explanations.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 5A: All volume anomalies (ratio > 2.0) with event context
-- ---------------------------------------------------------------------------
-- Lists every day where volume exceeded 2x the 30-day average, joined with
-- any market events within ±2 days to provide context.

SELECT
    a.symbol,
    dm.date,
    ROUND(dm.volume_ratio_30d::NUMERIC, 2)       AS vol_ratio,
    dp.volume_usd,
    ROUND(dm.daily_return_pct::NUMERIC, 2)        AS return_pct,
    ROUND(dm.daily_range_pct::NUMERIC, 2)         AS range_pct,
    me.title                                       AS nearby_event,
    me.event_type,
    (dm.date - me.event_date)                      AS days_from_event
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
JOIN daily_prices dp ON dp.asset_id = dm.asset_id AND dp.date = dm.date
LEFT JOIN market_events me
    ON dm.date BETWEEN me.event_date - INTERVAL '2 days'
                    AND me.event_date + INTERVAL '2 days'
    AND (
        me.affected_assets LIKE '%ALL%'
        OR me.affected_assets LIKE '%' || a.symbol || '%'
    )
WHERE dm.volume_ratio_30d > 2.0
ORDER BY dm.volume_ratio_30d DESC;

-- INTERPRETATION:
--   Rows with a nearby_event indicate volume spikes explained by known events.
--   Rows without a nearby event are "unexplained" — potentially worth
--   investigating for undocumented news, whale activity, or technical signals.


-- ---------------------------------------------------------------------------
-- QUERY 5B: Volume anomaly frequency per asset
-- ---------------------------------------------------------------------------
-- How often does each asset experience volume spikes? Higher frequency may
-- indicate a more event-driven or speculative asset.

SELECT
    a.symbol,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 3.0) AS extreme_spikes_3x,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 2.0) AS strong_spikes_2x,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 1.5) AS moderate_spikes_1_5x,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d IS NOT NULL) AS total_days,
    ROUND(
        COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 2.0)::NUMERIC
        / NULLIF(COUNT(*) FILTER (WHERE dm.volume_ratio_30d IS NOT NULL), 0) * 100,
        1
    ) AS pct_days_above_2x
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
GROUP BY a.symbol
ORDER BY pct_days_above_2x DESC;

-- EXPECTED: SOL and smaller altcoins likely have higher spike frequency
-- than BTC. BTC's volume is more stable due to deeper liquidity.


-- ---------------------------------------------------------------------------
-- QUERY 5C: Volume spikes with price direction — breakout vs breakdown
-- ---------------------------------------------------------------------------
-- Classifies volume spikes as potential breakouts (positive return) or
-- breakdowns (negative return). A high-volume up day suggests strong buying;
-- a high-volume down day suggests panic selling.

SELECT
    a.symbol,
    CASE
        WHEN dm.daily_return_pct > 2.0  THEN 'STRONG_BREAKOUT'
        WHEN dm.daily_return_pct > 0    THEN 'MILD_BREAKOUT'
        WHEN dm.daily_return_pct < -2.0 THEN 'STRONG_BREAKDOWN'
        WHEN dm.daily_return_pct < 0    THEN 'MILD_BREAKDOWN'
        ELSE 'NEUTRAL'
    END AS spike_type,
    COUNT(*)                                    AS occurrences,
    ROUND(AVG(dm.volume_ratio_30d)::NUMERIC, 2) AS avg_vol_ratio,
    ROUND(AVG(dm.daily_return_pct)::NUMERIC, 2)  AS avg_return_pct
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE dm.volume_ratio_30d > 2.0
  AND dm.daily_return_pct IS NOT NULL
GROUP BY a.symbol,
    CASE
        WHEN dm.daily_return_pct > 2.0  THEN 'STRONG_BREAKOUT'
        WHEN dm.daily_return_pct > 0    THEN 'MILD_BREAKOUT'
        WHEN dm.daily_return_pct < -2.0 THEN 'STRONG_BREAKDOWN'
        WHEN dm.daily_return_pct < 0    THEN 'MILD_BREAKDOWN'
        ELSE 'NEUTRAL'
    END
ORDER BY a.symbol, occurrences DESC;


-- ---------------------------------------------------------------------------
-- QUERY 5D: Monthly volume anomaly heatmap
-- ---------------------------------------------------------------------------
-- Counts volume spikes by month and year. Reveals seasonal patterns and
-- event clustering.

SELECT
    dd.year,
    dd.month,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 2.0) AS spikes_2x,
    COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 3.0) AS spikes_3x,
    ROUND(
        AVG(dm.volume_ratio_30d) FILTER (WHERE dm.volume_ratio_30d > 2.0)::NUMERIC,
        2
    ) AS avg_spike_ratio
FROM daily_metrics dm
JOIN date_dim dd ON dd.date_id = dm.date
WHERE dm.volume_ratio_30d IS NOT NULL
GROUP BY dd.year, dd.month
HAVING COUNT(*) FILTER (WHERE dm.volume_ratio_30d > 2.0) > 0
ORDER BY dd.year, dd.month;

-- INTERPRETATION:
--   Heavy spike months: May 2022 (Terra), Nov 2022 (FTX), Jan 2024 (ETF),
--   Nov 2024 (election), Oct 2025 (tariff crash).


-- ---------------------------------------------------------------------------
-- QUERY 5E: What happens AFTER a volume spike? (Next 5 days performance)
-- ---------------------------------------------------------------------------
-- For each volume spike event, look at the asset's return over the next
-- 1, 3, and 5 days. This reveals whether spikes are followed by
-- continuation (momentum) or reversal (mean reversion).

WITH spikes AS (
    SELECT
        dm.asset_id,
        a.symbol,
        dm.date AS spike_date,
        dm.volume_ratio_30d,
        dm.daily_return_pct AS spike_day_return
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id
    WHERE dm.volume_ratio_30d > 2.0
      AND dm.daily_return_pct IS NOT NULL
),

forward_returns AS (
    SELECT
        s.symbol,
        s.spike_date,
        s.volume_ratio_30d,
        s.spike_day_return,
        -- 1-day forward return
        (SELECT dm2.daily_return_pct
         FROM daily_metrics dm2
         WHERE dm2.asset_id = s.asset_id
           AND dm2.date > s.spike_date
           AND dm2.daily_return_pct IS NOT NULL
         ORDER BY dm2.date
         LIMIT 1) AS return_next_1d,
        -- Cumulative 3-day forward return (approximate via sum)
        (SELECT SUM(dm3.daily_return_pct)
         FROM (
             SELECT daily_return_pct
             FROM daily_metrics
             WHERE asset_id = s.asset_id
               AND date > s.spike_date
               AND daily_return_pct IS NOT NULL
             ORDER BY date
             LIMIT 3
         ) dm3) AS return_next_3d,
        -- Cumulative 5-day forward return
        (SELECT SUM(dm5.daily_return_pct)
         FROM (
             SELECT daily_return_pct
             FROM daily_metrics
             WHERE asset_id = s.asset_id
               AND date > s.spike_date
               AND daily_return_pct IS NOT NULL
             ORDER BY date
             LIMIT 5
         ) dm5) AS return_next_5d
    FROM spikes s
)

SELECT
    CASE
        WHEN spike_day_return > 0 THEN 'UP_SPIKE'
        ELSE 'DOWN_SPIKE'
    END AS spike_direction,
    COUNT(*) AS events,
    ROUND(AVG(return_next_1d)::NUMERIC, 2) AS avg_1d_fwd,
    ROUND(AVG(return_next_3d)::NUMERIC, 2) AS avg_3d_fwd,
    ROUND(AVG(return_next_5d)::NUMERIC, 2) AS avg_5d_fwd
FROM forward_returns
GROUP BY
    CASE
        WHEN spike_day_return > 0 THEN 'UP_SPIKE'
        ELSE 'DOWN_SPIKE'
    END
ORDER BY spike_direction;

-- INTERPRETATION:
--   If DOWN_SPIKE shows positive forward returns → mean reversion (typical).
--   If UP_SPIKE shows positive forward returns → momentum continuation.
--   This analysis is commonly used in quantitative trading research.
