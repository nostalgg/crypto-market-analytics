-- ============================================================================
-- Q7: Event-Driven Analysis — Market Behavior Around Known Events
-- File: 07_event_driven_analysis.sql
-- Business Question: How do prices, volatility, and volume behave before and
--                    after major market events? Do crash events cause lasting
--                    regime shifts, or do markets recover quickly?
-- SQL Features: JOIN with market_events, date arithmetic, window functions,
--               conditional aggregation with FILTER, LATERAL subqueries
-- ============================================================================
--
-- TECHNIQUE: Before/After Event Windows
-- =======================================
-- For each event in market_events, we examine a window of days before and
-- after the event date. Comparing pre-event vs post-event metrics reveals:
--   - Whether volatility spikes were anticipated or reactive
--   - Whether volume surges preceded or followed the event
--   - How long it took for metrics to normalize
--
-- This is the standard "event study" methodology used in financial research,
-- adapted here for crypto market events.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 7A: Pre/post event summary — 30-day windows around each event
-- ---------------------------------------------------------------------------
-- For each event, computes the average return, volatility, and volume ratio
-- in the 30 days before and 30 days after the event.

SELECT
    me.event_id,
    me.event_date,
    me.event_type,
    me.title,
    a.symbol,

    -- Pre-event metrics (30 days before)
    ROUND(AVG(dm.daily_return_pct)
        FILTER (WHERE dm.date BETWEEN me.event_date - 30 AND me.event_date - 1)::NUMERIC, 4
    ) AS avg_return_pre_30d,
    ROUND(AVG(dm.vol_30d)
        FILTER (WHERE dm.date BETWEEN me.event_date - 30 AND me.event_date - 1)::NUMERIC, 4
    ) AS avg_vol_pre_30d,
    ROUND(AVG(dm.volume_ratio_30d)
        FILTER (WHERE dm.date BETWEEN me.event_date - 30 AND me.event_date - 1)::NUMERIC, 4
    ) AS avg_volratio_pre_30d,

    -- Event day metrics
    ROUND(dm_event.daily_return_pct::NUMERIC, 2) AS event_day_return,
    ROUND(dm_event.vol_30d::NUMERIC, 4)          AS event_day_vol,
    ROUND(dm_event.volume_ratio_30d::NUMERIC, 2) AS event_day_volratio,

    -- Post-event metrics (30 days after)
    ROUND(AVG(dm.daily_return_pct)
        FILTER (WHERE dm.date BETWEEN me.event_date + 1 AND me.event_date + 30)::NUMERIC, 4
    ) AS avg_return_post_30d,
    ROUND(AVG(dm.vol_30d)
        FILTER (WHERE dm.date BETWEEN me.event_date + 1 AND me.event_date + 30)::NUMERIC, 4
    ) AS avg_vol_post_30d,
    ROUND(AVG(dm.volume_ratio_30d)
        FILTER (WHERE dm.date BETWEEN me.event_date + 1 AND me.event_date + 30)::NUMERIC, 4
    ) AS avg_volratio_post_30d

FROM market_events me
CROSS JOIN assets a
JOIN daily_metrics dm
    ON dm.asset_id = a.asset_id
    AND dm.date BETWEEN me.event_date - 30 AND me.event_date + 30
LEFT JOIN daily_metrics dm_event
    ON dm_event.asset_id = a.asset_id
    AND dm_event.date = me.event_date
WHERE
    me.affected_assets LIKE '%ALL%'
    OR me.affected_assets LIKE '%' || a.symbol || '%'
GROUP BY me.event_id, me.event_date, me.event_type, me.title,
         a.symbol, a.asset_id,
         dm_event.daily_return_pct, dm_event.vol_30d, dm_event.volume_ratio_30d
ORDER BY me.event_date, a.symbol;

-- INTERPRETATION:
--   Compare avg_vol_pre_30d vs avg_vol_post_30d to see if the event caused
--   a lasting volatility shift. Crash events typically show:
--     - Low vol before → spike on event day → elevated vol for weeks after.
--   Protocol upgrades (Merge, Shanghai) often show:
--     - Elevated vol before (anticipation) → declining vol after (resolution).


-- ---------------------------------------------------------------------------
-- QUERY 7B: Crash events — cumulative return in the 7/14/30 days after
-- ---------------------------------------------------------------------------
-- Specifically focuses on crash events. How fast did each asset decline,
-- and what was the total damage over different horizons?

WITH crash_events AS (
    SELECT event_id, event_date, title
    FROM market_events
    WHERE event_type = 'crash'
),

post_crash_returns AS (
    SELECT
        ce.event_id,
        ce.event_date,
        ce.title AS event_title,
        a.symbol,
        -- Sum of daily returns as approximate cumulative return
        ROUND(SUM(dm.daily_return_pct)
            FILTER (WHERE dm.date BETWEEN ce.event_date AND ce.event_date + 7)::NUMERIC, 2
        ) AS cum_return_7d,
        ROUND(SUM(dm.daily_return_pct)
            FILTER (WHERE dm.date BETWEEN ce.event_date AND ce.event_date + 14)::NUMERIC, 2
        ) AS cum_return_14d,
        ROUND(SUM(dm.daily_return_pct)
            FILTER (WHERE dm.date BETWEEN ce.event_date AND ce.event_date + 30)::NUMERIC, 2
        ) AS cum_return_30d,
        -- Max single-day loss in the 7 days post-event
        ROUND(MIN(dm.daily_return_pct)
            FILTER (WHERE dm.date BETWEEN ce.event_date AND ce.event_date + 7)::NUMERIC, 2
        ) AS worst_day_7d,
        -- Volume spike on event day
        ROUND(MAX(dm.volume_ratio_30d)
            FILTER (WHERE dm.date BETWEEN ce.event_date AND ce.event_date + 2)::NUMERIC, 2
        ) AS peak_vol_ratio_3d
    FROM crash_events ce
    CROSS JOIN assets a
    JOIN daily_metrics dm
        ON dm.asset_id = a.asset_id
        AND dm.date BETWEEN ce.event_date AND ce.event_date + 30
    GROUP BY ce.event_id, ce.event_date, ce.title, a.symbol, a.asset_id
)

SELECT
    event_date,
    event_title,
    symbol,
    cum_return_7d,
    cum_return_14d,
    cum_return_30d,
    worst_day_7d,
    peak_vol_ratio_3d
FROM post_crash_returns
ORDER BY event_date, cum_return_7d ASC;

-- EXPECTED:
--   FTX crash (2022-11-08): SOL should show the deepest 7d and 30d losses.
--   Terra crash (2022-05-09): Broad-based but moderate per-asset (contagion).
--   Tariff crash (2025-10-10): Largest liquidation, broad altcoin damage.


-- ---------------------------------------------------------------------------
-- QUERY 7C: Bitcoin halving — before/after performance comparison
-- ---------------------------------------------------------------------------
-- Examines BTC performance in specific windows around the April 2024 halving.
-- Historical pattern: price tends to rally 6-18 months after halving.

WITH halving AS (
    SELECT event_date
    FROM market_events
    WHERE event_type = 'halving'
    LIMIT 1
)

SELECT
    period,
    COUNT(*) AS trading_days,
    ROUND(SUM(dm.daily_return_pct)::NUMERIC, 2) AS cum_return,
    ROUND(AVG(dm.daily_return_pct)::NUMERIC, 4) AS avg_daily_return,
    ROUND(AVG(dm.vol_30d)::NUMERIC, 4)          AS avg_vol_30d,
    ROUND(AVG(dm.volume_ratio_30d)::NUMERIC, 2) AS avg_vol_ratio
FROM halving h
CROSS JOIN LATERAL (
    -- Define analysis periods
    SELECT dm.*, '90d_before' AS period
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id AND a.symbol = 'BTC'
    WHERE dm.date BETWEEN h.event_date - 90 AND h.event_date - 1

    UNION ALL

    SELECT dm.*, '90d_after' AS period
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id AND a.symbol = 'BTC'
    WHERE dm.date BETWEEN h.event_date + 1 AND h.event_date + 90

    UNION ALL

    SELECT dm.*, '180d_after' AS period
    FROM daily_metrics dm
    JOIN assets a ON a.asset_id = dm.asset_id AND a.symbol = 'BTC'
    WHERE dm.date BETWEEN h.event_date + 91 AND h.event_date + 180
) dm ON TRUE
GROUP BY period
ORDER BY
    CASE period
        WHEN '90d_before' THEN 1
        WHEN '90d_after'  THEN 2
        WHEN '180d_after' THEN 3
    END;

-- INTERPRETATION:
--   If avg_daily_return increases in post-halving periods, it supports the
--   supply-shock narrative. The 2024 halving was unique because BTC had
--   already set an ATH before the halving (ETF-driven demand).


-- ---------------------------------------------------------------------------
-- QUERY 7D: Protocol upgrades — ETH behavior around Merge and Shanghai
-- ---------------------------------------------------------------------------
-- Compares ETH volatility and return profile 30 days before vs 30 days after
-- the two major Ethereum upgrades.

SELECT
    me.title AS upgrade,
    me.event_date,

    -- 30 days before
    ROUND(AVG(dm.daily_return_pct)
        FILTER (WHERE dm.date < me.event_date)::NUMERIC, 4
    ) AS avg_return_before,
    ROUND(AVG(dm.vol_30d)
        FILTER (WHERE dm.date < me.event_date)::NUMERIC, 4
    ) AS avg_vol_before,
    ROUND(AVG(dm.volume_ratio_30d)
        FILTER (WHERE dm.date < me.event_date)::NUMERIC, 2
    ) AS avg_volratio_before,

    -- 30 days after
    ROUND(AVG(dm.daily_return_pct)
        FILTER (WHERE dm.date > me.event_date)::NUMERIC, 4
    ) AS avg_return_after,
    ROUND(AVG(dm.vol_30d)
        FILTER (WHERE dm.date > me.event_date)::NUMERIC, 4
    ) AS avg_vol_after,
    ROUND(AVG(dm.volume_ratio_30d)
        FILTER (WHERE dm.date > me.event_date)::NUMERIC, 2
    ) AS avg_volratio_after,

    -- Event day
    ROUND(MAX(dm.daily_return_pct)
        FILTER (WHERE dm.date = me.event_date)::NUMERIC, 2
    ) AS event_day_return

FROM market_events me
JOIN assets a ON a.symbol = 'ETH'
JOIN daily_metrics dm
    ON dm.asset_id = a.asset_id
    AND dm.date BETWEEN me.event_date - 30 AND me.event_date + 30
WHERE me.event_type = 'protocol_upgrade'
GROUP BY me.event_id, me.title, me.event_date
ORDER BY me.event_date;

-- EXPECTED:
--   Merge (Sep 2022): Sell-the-news pattern — ETH declined ~15% post-Merge.
--   Shanghai (Apr 2023): Buy-the-news — ETH rose despite staking unlock fears.


-- ---------------------------------------------------------------------------
-- QUERY 7E: Event type comparison — average market impact by event category
-- ---------------------------------------------------------------------------
-- Aggregates across all events to show which event types have the most
-- impact on returns, volatility, and volume.

SELECT
    me.event_type,
    COUNT(DISTINCT me.event_id) AS num_events,

    -- Average event-day return across all affected assets
    ROUND(AVG(dm.daily_return_pct)::NUMERIC, 2) AS avg_event_day_return,

    -- Average max volume ratio in the 3-day event window
    ROUND(AVG(
        CASE WHEN dm.date BETWEEN me.event_date AND me.event_date + 2
             THEN dm.volume_ratio_30d END
    )::NUMERIC, 2) AS avg_vol_ratio_3d,

    -- Average volatility change: post minus pre
    ROUND(
        AVG(dm.vol_30d) FILTER (WHERE dm.date BETWEEN me.event_date + 1 AND me.event_date + 14)
        - AVG(dm.vol_30d) FILTER (WHERE dm.date BETWEEN me.event_date - 14 AND me.event_date - 1),
        4
    )::NUMERIC AS vol_change_post_minus_pre

FROM market_events me
CROSS JOIN assets a
JOIN daily_metrics dm
    ON dm.asset_id = a.asset_id
    AND dm.date BETWEEN me.event_date - 14 AND me.event_date + 14
WHERE
    me.affected_assets LIKE '%ALL%'
    OR me.affected_assets LIKE '%' || a.symbol || '%'
GROUP BY me.event_type
ORDER BY avg_event_day_return ASC;

-- INTERPRETATION:
--   crash events should show the most negative avg_event_day_return and
--   the highest volume ratios. protocol_upgrade events may show elevated
--   vol before (anticipation) but moderate returns. market_milestone events
--   should show positive returns but the sell-the-news effect may dampen them.


-- ---------------------------------------------------------------------------
-- QUERY 7F: Correlation regime shift around crashes
-- ---------------------------------------------------------------------------
-- During crashes, correlations tend to spike toward 1.0 (everything sells off
-- together). This query compares the BTC-vs-altcoin correlation in the 30 days
-- before and after each crash event.

WITH crash_windows AS (
    SELECT
        me.event_id,
        me.event_date,
        me.title,
        a.symbol AS alt_symbol,
        CORR(dm_btc.daily_return_pct, dm_alt.daily_return_pct)
            FILTER (WHERE dm_btc.date BETWEEN me.event_date - 30 AND me.event_date - 1)
            AS corr_pre_30d,
        CORR(dm_btc.daily_return_pct, dm_alt.daily_return_pct)
            FILTER (WHERE dm_btc.date BETWEEN me.event_date AND me.event_date + 30)
            AS corr_post_30d
    FROM market_events me
    CROSS JOIN assets a
    JOIN assets btc ON btc.symbol = 'BTC'
    JOIN daily_metrics dm_btc
        ON dm_btc.asset_id = btc.asset_id
        AND dm_btc.date BETWEEN me.event_date - 30 AND me.event_date + 30
    JOIN daily_metrics dm_alt
        ON dm_alt.asset_id = a.asset_id
        AND dm_alt.date = dm_btc.date
    WHERE me.event_type = 'crash'
      AND a.symbol != 'BTC'
      AND dm_btc.daily_return_pct IS NOT NULL
      AND dm_alt.daily_return_pct IS NOT NULL
    GROUP BY me.event_id, me.event_date, me.title, a.symbol
)

SELECT
    event_date,
    title,
    alt_symbol,
    ROUND(corr_pre_30d::NUMERIC, 3)  AS corr_before_crash,
    ROUND(corr_post_30d::NUMERIC, 3) AS corr_during_crash,
    ROUND((corr_post_30d - corr_pre_30d)::NUMERIC, 3) AS corr_change
FROM crash_windows
ORDER BY event_date, corr_change DESC;

-- INTERPRETATION:
--   Positive corr_change means correlations increased during the crash
--   ("flight to correlation"). This is the standard finding in financial
--   literature: diversification benefits disappear during crises.
--   Assets that show NEGATIVE corr_change during crashes are rare and
--   analytically interesting (potential safe-haven or unique dynamics).
