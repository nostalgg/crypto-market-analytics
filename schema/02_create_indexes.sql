-- ============================================================================
-- Crypto Market Analytics Data Warehouse -- Index DDL
-- File: 02_create_indexes.sql
-- Phase: 1 -- Foundation
-- Database: PostgreSQL
-- ============================================================================
--
-- INDEX STRATEGY FOR TIME-SERIES FINANCIAL DATA
-- ==============================================
--
-- The core access pattern in this warehouse is time-series retrieval:
-- nearly every analytical query filters or joins on (asset_id, date) or
-- scans a date range. The indexes below are designed for these patterns:
--
-- 1. COMPOSITE INDEX (asset_id, date) on daily_prices and daily_metrics
--    This is the PRIMARY access pattern. Analytical queries almost always
--    filter by a specific asset and then scan a date range, e.g.:
--      WHERE asset_id = 1 AND date BETWEEN '2024-01-01' AND '2024-12-31'
--    The composite index supports both equality on asset_id and range scan
--    on date in a single B-tree traversal. This directly serves:
--      - Q1: Momentum streak detection (single asset, date-ordered)
--      - Q2: Volatility regime analysis (single asset, rolling windows)
--      - Q4: Drawdown calculation (single asset, cumulative max over time)
--
-- 2. SINGLE-COLUMN INDEX on date (daily_prices)
--    Supports cross-asset date-range scans where we need all assets for
--    a given period, without filtering by a specific asset_id, e.g.:
--      WHERE date BETWEEN '2024-01-01' AND '2024-03-31'
--    This directly serves:
--      - Q3: Correlation analysis (all assets over a date window)
--      - Q6: Cross-asset weekly ranking (all assets per week)
--
-- 3. SINGLE-COLUMN INDEX on event_date (market_events)
--    Supports event-driven analysis queries that join events to price
--    data within a date window around each event, e.g.:
--      WHERE me.event_date BETWEEN '2024-01-01' AND '2024-12-31'
--    This directly serves:
--      - Phase 4 event queries (price behavior N days before/after events)
--
-- NOTE: The UNIQUE constraints on (asset_id, date) in daily_prices and
-- daily_metrics already create implicit unique indexes. The explicit
-- composite indexes below may be redundant with those unique indexes in
-- PostgreSQL (which implements UNIQUE via a unique B-tree index). They
-- are included here for clarity and to ensure the indexes exist with
-- predictable names regardless of how the UNIQUE constraint is
-- implemented internally.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Index 1: Composite index on daily_prices for single-asset time-series
-- ---------------------------------------------------------------------------
-- Supports the most common query pattern: retrieve price data for one asset
-- over a date range. Used by momentum (Q1), volatility (Q2), drawdown (Q4),
-- and volume anomaly (Q5) queries.

CREATE INDEX IF NOT EXISTS idx_daily_prices_asset_date
    ON daily_prices (asset_id, date);


-- ---------------------------------------------------------------------------
-- Index 2: Date-only index on daily_prices for cross-asset scans
-- ---------------------------------------------------------------------------
-- Supports queries that need all assets for a given date range without
-- specifying a particular asset_id. Used by correlation (Q3) and
-- cross-asset ranking (Q6) queries.

CREATE INDEX IF NOT EXISTS idx_daily_prices_date
    ON daily_prices (date);


-- ---------------------------------------------------------------------------
-- Index 3: Composite index on daily_metrics for single-asset lookups
-- ---------------------------------------------------------------------------
-- Mirrors the daily_prices composite index for the derived metrics table.
-- Analytical queries frequently join or scan daily_metrics by (asset, date)
-- to retrieve pre-computed volatility, returns, and moving averages.

CREATE INDEX IF NOT EXISTS idx_daily_metrics_asset_date
    ON daily_metrics (asset_id, date);


-- ---------------------------------------------------------------------------
-- Index 4: Date index on market_events for event-driven analysis
-- ---------------------------------------------------------------------------
-- Supports Phase 4 event-driven queries that scan events by date range
-- and then join to daily_prices/daily_metrics within a window around
-- each event (e.g., 30 days before and after a halving or crash).

CREATE INDEX IF NOT EXISTS idx_market_events_date
    ON market_events (event_date);
