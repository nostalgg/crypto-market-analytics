-- ============================================================================
-- Crypto Market Analytics Data Warehouse -- Schema DDL
-- File: 01_create_tables.sql
-- Phase: 1 -- Foundation
-- Database: PostgreSQL
-- ============================================================================
--
-- STAR SCHEMA DESIGN
-- ==================
--
-- This schema follows a star schema pattern optimized for time-series
-- financial analytics and OLAP workloads:
--
--   Dimension Tables:
--     - assets       : Asset dimension (what). Each row represents a single
--                       cryptocurrency with its static attributes (symbol,
--                       name, category, launch date).
--     - date_dim     : Date dimension (when). Pre-populated calendar table
--                       enabling efficient date-based filtering, weekend
--                       analysis, and period grouping without runtime
--                       date-part extraction.
--
--   Fact Tables:
--     - daily_prices : Core fact table at the grain of one row per asset
--                       per calendar day. Contains OHLCV market data and
--                       market capitalization. This is the primary source
--                       for all analytical queries.
--     - daily_metrics: Derived fact table at the same grain. Contains
--                       pre-computed technical indicators (returns,
--                       volatility, moving averages, volume ratios) to
--                       avoid recalculating expensive window functions on
--                       every query.
--
--   Event Table:
--     - market_events: Semi-structured reference table cataloging significant
--                       market events (crashes, halvings, regulatory actions,
--                       protocol upgrades) for event-driven analysis and
--                       before/after comparison queries.
--
-- GRAIN: daily_prices and daily_metrics share the same grain --
--        one row per (asset_id, date) combination, enforced by UNIQUE
--        constraints. Both tables reference the assets and date_dim
--        dimensions via foreign keys.
--
-- NUMERIC PRECISION: All price columns use NUMERIC(20,8) to preserve
--        sub-cent precision for low-priced assets. Volume and market cap
--        columns use NUMERIC(30,2) to accommodate trillion-dollar values.
--        FLOAT/DOUBLE PRECISION is deliberately avoided for financial data.
--
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 1. ASSETS -- Dimension Table (What)
-- ---------------------------------------------------------------------------
-- Each row represents one cryptocurrency tracked in this warehouse.
-- The symbol column is the natural key (UNIQUE); asset_id is the surrogate.
-- Category classifies the asset's role in the ecosystem (layer1, oracle, etc.)

CREATE TABLE IF NOT EXISTS assets (
    asset_id    SERIAL          PRIMARY KEY,
    symbol      VARCHAR(10)     UNIQUE NOT NULL,
    name        VARCHAR(100)    NOT NULL,
    category    VARCHAR(50),
    launch_date DATE,
    is_active   BOOLEAN         DEFAULT TRUE,
    created_at  TIMESTAMP       DEFAULT NOW()
);


-- ---------------------------------------------------------------------------
-- 2. DATE_DIM -- Date Dimension Table (When)
-- ---------------------------------------------------------------------------
-- Pre-populated calendar table covering the full date range (2022-01-01
-- through 2025-10-31). Crypto markets trade 365 days/year, so every
-- calendar day is included -- weekends are flagged with is_weekend.
--
-- This table enables efficient GROUP BY on year/quarter/month/week without
-- runtime EXTRACT() calls, and supports weekend-aware volume analysis.

CREATE TABLE IF NOT EXISTS date_dim (
    date_id     DATE            PRIMARY KEY,
    year        INT             NOT NULL,
    quarter     INT             NOT NULL,
    month       INT             NOT NULL,
    week        INT             NOT NULL,
    day_of_week INT             NOT NULL,
    is_weekend  BOOLEAN         NOT NULL,

    CONSTRAINT chk_date_dim_quarter     CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_date_dim_month       CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_date_dim_day_of_week CHECK (day_of_week BETWEEN 0 AND 6)
);


-- ---------------------------------------------------------------------------
-- 3. DAILY_PRICES -- Core Fact Table
-- ---------------------------------------------------------------------------
-- One row per asset per calendar day. Contains raw OHLCV market data from
-- CoinGecko. The UNIQUE constraint on (asset_id, date) enforces the grain
-- and prevents duplicate ingestion.
--
-- CHECK constraints ensure data integrity:
--   - close > 0   : A zero or negative closing price is invalid
--   - volume >= 0  : Volume can be zero (e.g., delisted/halted) but not negative

CREATE TABLE IF NOT EXISTS daily_prices (
    price_id        SERIAL          PRIMARY KEY,
    asset_id        INT             NOT NULL
                                    REFERENCES assets(asset_id),
    date            DATE            NOT NULL
                                    REFERENCES date_dim(date_id),
    open            NUMERIC(20,8),
    high            NUMERIC(20,8),
    low             NUMERIC(20,8),
    close           NUMERIC(20,8)   NOT NULL,
    volume_usd      NUMERIC(30,2),
    market_cap_usd  NUMERIC(30,2),

    CONSTRAINT uq_daily_prices_asset_date   UNIQUE (asset_id, date),
    CONSTRAINT chk_daily_prices_close       CHECK (close > 0),
    CONSTRAINT chk_daily_prices_volume      CHECK (volume_usd >= 0)
);


-- ---------------------------------------------------------------------------
-- 4. DAILY_METRICS -- Derived Fact Table
-- ---------------------------------------------------------------------------
-- Pre-computed technical indicators at the same grain as daily_prices.
-- Populated by the Python ingestion pipeline after daily_prices is loaded.
--
-- Storing derived metrics avoids recalculating expensive window functions
-- (rolling STDDEV, moving averages) on every analytical query. The metrics
-- here directly support the six core business questions:
--
--   daily_return_pct  : Q1 momentum/reversal, Q6 ranking
--   daily_range_pct   : Intraday volatility measure
--   vol_7d / vol_30d  : Q2 volatility regimes
--   sma_7 / sma_30    : Trend indicators for regime detection
--   volume_ratio_30d  : Q5 volume anomaly detection (values > 2.0 = spike)

CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_id           SERIAL          PRIMARY KEY,
    asset_id            INT             NOT NULL
                                        REFERENCES assets(asset_id),
    date                DATE            NOT NULL
                                        REFERENCES date_dim(date_id),
    daily_return_pct    NUMERIC(10,6),
    daily_range_pct     NUMERIC(10,6),
    vol_7d              NUMERIC(10,6),
    vol_30d             NUMERIC(10,6),
    sma_7               NUMERIC(20,8),
    sma_30              NUMERIC(20,8),
    volume_ratio_30d    NUMERIC(10,4),

    CONSTRAINT uq_daily_metrics_asset_date UNIQUE (asset_id, date)
);


-- ---------------------------------------------------------------------------
-- 5. MARKET_EVENTS -- Event Reference Table
-- ---------------------------------------------------------------------------
-- Catalogs significant market events for event-driven analysis (Phase 4).
-- Each event has a type classification validated by CHECK constraint,
-- enabling queries like "BTC price behavior 30 days post-halving" or
-- "volume spikes around crash events."
--
-- affected_assets is a descriptive metadata column (not a FK). Convention:
--   - 'ALL'      : Event affected the entire market
--   - 'ALL,SOL'  : Market-wide but SOL disproportionately affected
--   - 'ETH'      : Only ETH materially affected

CREATE TABLE IF NOT EXISTS market_events (
    event_id        SERIAL          PRIMARY KEY,
    event_date      DATE            NOT NULL,
    event_type      VARCHAR(50)     NOT NULL,
    title           VARCHAR(200)    NOT NULL,
    description     TEXT,
    affected_assets VARCHAR(500),
    source_url      TEXT,

    CONSTRAINT chk_event_type CHECK (
        event_type IN (
            'halving',
            'crash',
            'regulatory',
            'protocol_upgrade',
            'market_milestone',
            'exchange_event',
            'macro_event'
        )
    )
);
