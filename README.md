# Crypto Market Analytics Data Warehouse

A PostgreSQL-based data warehouse for crypto market analytics, answering six core business questions using advanced SQL window functions, time-series analysis, and financial metrics computation.

## Business Questions

| # | Question | SQL Techniques | Query File |
|---|----------|----------------|------------|
| Q1 | Which assets had the longest consecutive up-day streaks? | LAG, gaps-and-islands pattern, conditional running count | `queries/01_momentum_streaks.sql` |
| Q2 | Can we identify distinct volatility regimes (low/medium/high/extreme)? | CASE classification, regime transition detection, date arithmetic | `queries/02_volatility_regimes.sql` |
| Q3 | How do BTC and other assets correlate, and when do they decorrelate? | CORR() on returns, self-join for pairs, rolling windows, FILTER | `queries/03_correlation_analysis.sql` |
| Q4 | What is the maximum peak-to-trough drawdown per asset? | Cumulative MAX via window function, running peak tracking | `queries/04_drawdown_analysis.sql` |
| Q5 | Which days had volume > 2x the 30-day average? | Threshold-based anomaly detection, JOIN with market_events | `queries/05_volume_anomalies.sql` |
| Q6 | How do assets rank against each other each week? | NTILE(4), RANK, DENSE_RANK, ROW_NUMBER, GROUPING SETS / ROLLUP | `queries/06_cross_asset_ranking.sql` |
| Bonus | How do prices and volatility behave around major market events? | Event study methodology, LATERAL subqueries, before/after windows | `queries/07_event_driven_analysis.sql` |

## Architecture

### Star Schema

```
                    ┌─────────────┐
                    │   assets    │  Dimension (What)
                    │  (8 rows)   │
                    └──────┬──────┘
                           │
    ┌──────────────┐       │       ┌───────────────┐
    │  date_dim    │───────┼───────│ daily_prices   │  Fact (OHLCV)
    │  (calendar)  │       │       │ (~11,200 rows) │
    └──────────────┘       │       └───────────────┘
                           │
                    ┌──────┴──────┐
                    │daily_metrics│  Derived Fact
                    │ (returns,   │  (pre-computed)
                    │  vol, SMA)  │
                    └─────────────┘

    ┌─────────────────┐
    │  market_events   │  Reference (18 events)
    │  (crash, halving │
    │   regulatory...) │
    └─────────────────┘
```

**Grain**: One row per asset per calendar day for both fact tables.

### Assets Tracked (8)

| Symbol | Category | Role in Analysis |
|--------|----------|------------------|
| BTC | layer1 | Benchmark, halving events, ETF milestone |
| ETH | layer1 | Merge/Shanghai upgrades, ETF comparison |
| SOL | layer1 | High-beta, FTX collapse recovery |
| BNB | layer1_exchange | Exchange token, regulatory events |
| ADA | layer1 | Large-cap laggard, contrast asset |
| AVAX | layer1 | Mid-cap DeFi ecosystem |
| LINK | oracle | Non-L1 diversifier, integration-driven |
| DOT | layer1_interop | Underperformer, drawdown archetype |

**Data source**: Yahoo Finance via `yfinance` (full daily OHLCV, no API key required)
**Timeframe**: 2022-01-01 to 2025-10-31 (~1,400 daily records per asset)

## Technical Stack

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL |
| Language | Python 3.10+ |
| Data Source | Yahoo Finance (`yfinance`) |
| Financial Precision | `NUMERIC` types (never `FLOAT`) |
| Volatility | STDDEV_POP, annualized with √365 |
| Correlation | Pearson on returns (not prices) |

## Project Structure

```
SQL/
├── README.md                              # This file
├── CLAUDE.md                              # Project configuration
├── config/
│   └── config.example.env                 # Environment variables template
├── schema/
│   ├── 01_create_tables.sql               # DDL: 5 tables (star schema)
│   └── 02_create_indexes.sql              # 4 strategic indexes
├── scripts/
│   ├── ingest_yahoo.py                    # Yahoo Finance → daily_prices
│   ├── compute_metrics.py                 # 7 derived metrics → daily_metrics
│   └── populate_events.py                 # 18 market events → market_events
├── queries/
│   ├── 00_data_integrity_checks.sql       # 30+ validation checks
│   ├── 01_momentum_streaks.sql            # Q1: Streak detection
│   ├── 02_volatility_regimes.sql          # Q2: Regime classification
│   ├── 03_correlation_analysis.sql        # Q3: Cross-asset correlation
│   ├── 04_drawdown_analysis.sql           # Q4: Peak-to-trough decline
│   ├── 05_volume_anomalies.sql            # Q5: Volume spike detection
│   ├── 06_cross_asset_ranking.sql         # Q6: Weekly asset rankings
│   └── 07_event_driven_analysis.sql       # Bonus: Event study analysis
├── deliverables/
│   ├── phase1_domain_validation.md        # Asset selection & market events
│   └── phase2_formula_validation.md       # Formula specs for all metrics
└── agents/                                # Multi-agent system definitions
    ├── orchestrator.md
    ├── prompt_analysis_expert.md
    ├── finance_domain_expert.md
    ├── sql_datawarehouse_expert.md
    └── python_financial_expert.md
```

## Setup & Usage

### Prerequisites

- PostgreSQL 14+
- Python 3.10+
- `pip install psycopg2-binary python-dotenv yfinance pandas`

### 1. Configure Environment

```bash
cp config/config.example.env .env
# Edit .env with your PostgreSQL credentials
```

### 2. Create Schema

```bash
psql -d crypto_analytics -f schema/01_create_tables.sql
psql -d crypto_analytics -f schema/02_create_indexes.sql
```

### 3. Ingest Data

```bash
python scripts/ingest_yahoo.py          # Load OHLCV from Yahoo Finance
python scripts/compute_metrics.py       # Compute 7 derived metrics
python scripts/populate_events.py       # Insert 18 market events
```

### 4. Validate

```bash
psql -d crypto_analytics -f queries/00_data_integrity_checks.sql
```

### 5. Run Analytical Queries

```bash
psql -d crypto_analytics -f queries/01_momentum_streaks.sql
# ... through 07_event_driven_analysis.sql
```

## Derived Metrics

All 7 metrics are pre-computed in `daily_metrics` to avoid recalculating window functions on every query:

| Metric | Formula | Notes |
|--------|---------|-------|
| `daily_return_pct` | `(close_t - close_{t-1}) / close_{t-1} * 100` | Simple return as percentage points |
| `daily_range_pct` | `(high - low) / low * 100` | Intraday range, always ≥ 0 |
| `vol_7d` | `STDDEV_POP(return) OVER (7-day window)` | Population std dev, not annualized |
| `vol_30d` | `STDDEV_POP(return) OVER (30-day window)` | Population std dev, not annualized |
| `sma_7` | `AVG(close) OVER (7-day window)` | Short-term trend indicator |
| `sma_30` | `AVG(close) OVER (30-day window)` | Medium-term trend indicator |
| `volume_ratio_30d` | `volume / AVG(prior 30d volume)` | >2.0 = anomaly. Current day excluded from denominator |

Metrics are `NULL` when the window is incomplete (e.g., `vol_30d` is `NULL` for the first 30 days per asset).

## Market Events (18)

Events span the 2022-2025 analysis window and cover the full market cycle:

| Type | Count | Examples |
|------|-------|---------|
| crash | 4 | Terra/LUNA, FTX collapse, Trump tariff crash |
| market_milestone | 6 | BTC ETF approval, $100K milestone, election rally |
| protocol_upgrade | 2 | Ethereum Merge, Shanghai upgrade |
| regulatory | 2 | SEC lawsuits, Trump crypto executive order |
| exchange_event | 2 | Three Arrows Capital, FTX exposure |
| halving | 1 | Fourth Bitcoin halving (Apr 2024) |
| macro_event | 1 | SVB banking crisis |

## SQL Techniques Demonstrated

- **Window Functions**: LAG, LEAD, ROW_NUMBER, RANK, DENSE_RANK, NTILE, running MAX, STDDEV_POP, CORR, rolling AVG
- **Time-Series Patterns**: Gaps-and-islands (streak detection), running peak for drawdown, rolling window frames
- **Aggregation**: FILTER clause, GROUPING SETS, ROLLUP, conditional aggregation with CASE
- **Joins**: Self-join for correlation pairs, CROSS JOIN for all-pairs, LATERAL for event windows, LEFT JOIN for optional event context
- **Data Warehouse Design**: Star schema, dimension/fact tables, pre-computed metrics, composite indexes for time-series access patterns

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Returns stored as percentage | `5.0` for 5% gain | Matches `_pct` column name convention; downstream volatility in intuitive percentage-point units |
| STDDEV_POP over STDDEV_SAMP | Population formula (÷ N) | Rolling window IS the population, not a sample. Matches Bloomberg HVOL convention |
| Volume ratio excludes current day | `ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING` | Prevents today's spike from contaminating its own baseline |
| √365 for annualization | Crypto trades every day | √252 is for equities with ~252 trading days/year |
| NUMERIC over FLOAT | `NUMERIC(20,8)` for prices | Financial data requires exact decimal arithmetic |

## Data Pipeline

All scripts are **idempotent** — safe to re-run without creating duplicates:

- `ingest_yahoo.py`: Uses `ON CONFLICT (asset_id, date) DO UPDATE` for upserts
- `compute_metrics.py`: Truncates `daily_metrics` and recomputes from `daily_prices`
- `populate_events.py`: Truncates `market_events` and re-inserts all 18 events
