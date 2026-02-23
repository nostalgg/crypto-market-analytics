"""
schema_context.py — Static schema description injected into every LLM prompt.

No imports, no runtime logic. This module exposes a single constant:
    SCHEMA_CONTEXT: str

The string contains all table DDLs, asset metadata, data range, NULL
warm-up warnings, and standard JOIN patterns. It is intentionally verbose
so the model can generate accurate SQL without hallucinating column names.
"""

SCHEMA_CONTEXT: str = """
=== DATABASE: crypto_analytics (PostgreSQL) ===

--- DATA RANGE ---
All market data covers 2022-01-01 through 2025-10-31 (inclusive).
Crypto markets trade every day of the year; every calendar date in that
range has rows in daily_prices and daily_metrics for each tracked asset.

--- TRACKED ASSETS (assets.symbol) ---
BTC  (Bitcoin      — layer1)
ETH  (Ethereum     — layer1)
SOL  (Solana       — layer1)
BNB  (BNB          — exchange_token)
ADA  (Cardano      — layer1)
AVAX (Avalanche    — layer1)
LINK (Chainlink    — oracle)
DOT  (Polkadot     — layer1)

--- TABLE 1: assets (dimension) ---
Column       | Type          | Nullable | Notes
-------------|---------------|----------|------
asset_id     | SERIAL        | NOT NULL | Primary key (surrogate)
symbol       | VARCHAR(10)   | NOT NULL | Natural key — BTC, ETH, SOL, …
name         | VARCHAR(100)  | NOT NULL | Full name (e.g. "Bitcoin")
category     | VARCHAR(50)   | NULL     | layer1, oracle, exchange_token
launch_date  | DATE          | NULL     |
is_active    | BOOLEAN       | NOT NULL | DEFAULT TRUE
created_at   | TIMESTAMP     | NOT NULL | DEFAULT NOW()

--- TABLE 2: date_dim (dimension) ---
Column       | Type  | Nullable | Notes
-------------|-------|----------|------
date_id      | DATE  | NOT NULL | Primary key — join ON dp.date = d.date_id
year         | INT   | NOT NULL |
quarter      | INT   | NOT NULL | 1–4
month        | INT   | NOT NULL | 1–12
week         | INT   | NOT NULL | ISO week number
day_of_week  | INT   | NOT NULL | 0=Sunday … 6=Saturday
is_weekend   | BOOL  | NOT NULL |

--- TABLE 3: daily_prices (fact — one row per asset per day) ---
Column         | Type           | Nullable | Notes
---------------|----------------|----------|------
price_id       | SERIAL         | NOT NULL | Primary key
asset_id       | INT            | NOT NULL | FK → assets.asset_id
date           | DATE           | NOT NULL | FK → date_dim.date_id
open           | NUMERIC(20,8)  | NULL     |
high           | NUMERIC(20,8)  | NULL     |
low            | NUMERIC(20,8)  | NULL     |
close          | NUMERIC(20,8)  | NOT NULL | Always positive (CHECK > 0)
volume_usd     | NUMERIC(30,2)  | NULL     |
market_cap_usd | NUMERIC(30,2)  | NULL     | *** ALWAYS NULL — not populated ***

UNIQUE constraint: (asset_id, date)

*** IMPORTANT NULL WARNING ***
market_cap_usd is NEVER populated. Do NOT use it in queries.

--- TABLE 4: daily_metrics (derived fact — same grain as daily_prices) ---
Column            | Type          | Nullable | Notes
------------------|---------------|----------|------
metric_id         | SERIAL        | NOT NULL | Primary key
asset_id          | INT           | NOT NULL | FK → assets.asset_id
date              | DATE          | NOT NULL | FK → date_dim.date_id
daily_return_pct  | NUMERIC(10,6) | NULL     | (close_t / close_t-1 - 1) * 100
daily_range_pct   | NUMERIC(10,6) | NULL     | (high - low) / close * 100
vol_7d            | NUMERIC(10,6) | NULL     | 7-day rolling stdev of daily_return_pct
vol_30d           | NUMERIC(10,6) | NULL     | 30-day rolling stdev of daily_return_pct
sma_7             | NUMERIC(20,8) | NULL     | 7-day simple moving average of close
sma_30            | NUMERIC(20,8) | NULL     | 30-day simple moving average of close
volume_ratio_30d  | NUMERIC(10,4) | NULL     | volume_usd / 30-day avg volume

UNIQUE constraint: (asset_id, date)

*** NULL WARM-UP PERIODS ***
daily_return_pct : NULL for the first row of each asset (no prior close)
vol_7d           : NULL for first 7 days of each asset's data
vol_30d          : NULL for first 30 days of each asset's data
sma_7            : NULL for first 7 days of each asset's data
sma_30           : NULL for first 30 days of each asset's data
volume_ratio_30d : NULL for first 30 days of each asset's data

Always add IS NOT NULL filters when selecting these columns to avoid
including warm-up NULLs in aggregations or time-series outputs.

--- TABLE 5: market_events (event reference) ---
Column           | Type          | Nullable | Notes
-----------------|---------------|----------|------
event_id         | SERIAL        | NOT NULL | Primary key
event_date       | DATE          | NOT NULL |
event_type       | VARCHAR(50)   | NOT NULL | halving, crash, regulatory,
                 |               |          | protocol_upgrade, market_milestone,
                 |               |          | exchange_event, macro_event
title            | VARCHAR(200)  | NOT NULL |
description      | TEXT          | NULL     |
affected_assets  | VARCHAR(500)  | NULL     | 'ALL', 'BTC', 'ETH', 'ALL,SOL', …
source_url       | TEXT          | NULL     |

--- STANDARD JOIN PATTERNS ---

-- Pattern A: asset + daily price time series
SELECT a.symbol, dp.date, dp.close
FROM   daily_prices dp
JOIN   assets a ON a.asset_id = dp.asset_id
WHERE  a.symbol = 'BTC'
ORDER  BY dp.date;

-- Pattern B: asset + pre-computed metrics time series
SELECT a.symbol, dm.date, dm.vol_30d, dm.daily_return_pct
FROM   daily_metrics dm
JOIN   assets a ON a.asset_id = dm.asset_id
WHERE  a.symbol = 'ETH'
  AND  dm.vol_30d IS NOT NULL
ORDER  BY dm.date;

-- Pattern C: multi-asset cross-section (all 8 assets)
SELECT a.symbol, AVG(dm.daily_return_pct) AS avg_return
FROM   daily_metrics dm
JOIN   assets a ON a.asset_id = dm.asset_id
WHERE  dm.daily_return_pct IS NOT NULL
GROUP  BY a.symbol
ORDER  BY avg_return DESC;

-- Pattern D: date_dim for period filtering
SELECT a.symbol, d.quarter, AVG(dp.close) AS avg_price
FROM   daily_prices dp
JOIN   assets a ON a.asset_id = dp.asset_id
JOIN   date_dim d ON d.date_id = dp.date
WHERE  d.year = 2024
GROUP  BY a.symbol, d.quarter
ORDER  BY a.symbol, d.quarter;

-- Pattern E: event-driven analysis (±N days around an event)
SELECT a.symbol, dp.date, dp.close,
       CASE WHEN dp.date < me.event_date THEN 'before' ELSE 'after' END AS period
FROM   market_events me
JOIN   assets a ON a.symbol = 'ETH'
JOIN   daily_prices dp
       ON dp.asset_id = a.asset_id
       AND dp.date BETWEEN me.event_date - 14 AND me.event_date + 14
WHERE  me.title ILIKE '%Merge%'
ORDER  BY dp.date;
"""
