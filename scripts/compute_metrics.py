"""
Compute Daily Metrics for Crypto Market Analytics Data Warehouse.

Purpose:
    Populates the daily_metrics table by computing all 7 technical indicators
    from the raw daily_prices data using a single SQL CTE chain executed via
    PostgreSQL window functions.  The SQL engine handles all rolling
    computations (STDDEV_POP, AVG, LAG) efficiently over ~10,000+ rows per
    asset, avoiding Python-side loops.

    Metrics computed:
        1. daily_return_pct  -- close-to-close simple return (percentage)
        2. daily_range_pct   -- intraday (high-low)/low (percentage)
        3. vol_7d            -- 7-day rolling realized volatility (STDDEV_POP)
        4. vol_30d           -- 30-day rolling realized volatility (STDDEV_POP)
        5. sma_7             -- 7-day simple moving average of close
        6. sma_30            -- 30-day simple moving average of close
        7. volume_ratio_30d  -- today's volume / avg of prior 30 days' volume

Usage:
    # Configure via .env file or environment variables:
    #   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

    python compute_metrics.py

Dependencies:
    psycopg2-binary  -- PostgreSQL adapter
    python-dotenv    -- Environment variable loading from .env files
"""

import logging
import os
import sys
import time
from typing import Optional

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("compute_metrics")


# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------
def get_db_connection() -> psycopg2.extensions.connection:
    """Create and return a PostgreSQL connection using environment variables.

    Expected env vars: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
    Raises ``psycopg2.OperationalError`` if the connection cannot be established.
    """
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    conn.autocommit = False
    logger.info(
        "Connected to PostgreSQL: %s@%s:%s/%s",
        os.environ["DB_USER"],
        os.environ["DB_HOST"],
        os.environ.get("DB_PORT", "5432"),
        os.environ["DB_NAME"],
    )
    return conn


# ---------------------------------------------------------------------------
# SQL CTE Chain -- All 7 Metrics in a Single Statement
# ---------------------------------------------------------------------------
METRICS_SQL: str = """
WITH base_data AS (
    -- CTE 1: Join daily_prices with assets to get all needed columns.
    -- This is the foundation for all metric calculations.
    SELECT
        dp.asset_id,
        dp.date,
        dp.open,
        dp.high,
        dp.low,
        dp.close,
        dp.volume_usd
    FROM daily_prices dp
    JOIN assets a ON a.asset_id = dp.asset_id
    WHERE a.is_active = TRUE
    ORDER BY dp.asset_id, dp.date
),

with_returns AS (
    -- CTE 2: Compute daily_return_pct and daily_range_pct.
    -- daily_return_pct uses LAG to get the previous close for the same asset.
    -- daily_range_pct uses (high - low) / low -- always non-negative.
    SELECT
        bd.asset_id,
        bd.date,
        bd.close,
        bd.volume_usd,

        -- Metric 1: daily_return_pct (percentage, e.g. 5.0 for +5%)
        -- NULL on the first day per asset (no previous close).
        CASE
            WHEN LAG(bd.close, 1) OVER (
                PARTITION BY bd.asset_id ORDER BY bd.date
            ) IS NOT NULL
            THEN (
                (bd.close - LAG(bd.close, 1) OVER (
                    PARTITION BY bd.asset_id ORDER BY bd.date
                ))
                / LAG(bd.close, 1) OVER (
                    PARTITION BY bd.asset_id ORDER BY bd.date
                )
                * 100
            )
            ELSE NULL
        END AS daily_return_pct,

        -- Metric 2: daily_range_pct (percentage)
        -- NULL only if high or low is NULL.
        CASE
            WHEN bd.high IS NOT NULL AND bd.low IS NOT NULL
            THEN (bd.high - bd.low) / bd.low * 100
            ELSE NULL
        END AS daily_range_pct

    FROM base_data bd
),

with_all_metrics AS (
    -- CTE 3: Compute vol_7d, vol_30d, sma_7, sma_30, volume_ratio_30d.
    -- All use window functions over with_returns.
    -- Each metric enforces minimum observations via CASE + COUNT.
    SELECT
        wr.asset_id,
        wr.date,
        wr.daily_return_pct,
        wr.daily_range_pct,

        -- Metric 3: vol_7d -- 7-day rolling realized volatility (STDDEV_POP)
        -- Requires exactly 7 non-NULL return values in the window.
        CASE
            WHEN COUNT(wr.daily_return_pct) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) = 7
            THEN STDDEV_POP(wr.daily_return_pct) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS vol_7d,

        -- Metric 4: vol_30d -- 30-day rolling realized volatility (STDDEV_POP)
        -- Requires exactly 30 non-NULL return values in the window.
        CASE
            WHEN COUNT(wr.daily_return_pct) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) = 30
            THEN STDDEV_POP(wr.daily_return_pct) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS vol_30d,

        -- Metric 5: sma_7 -- 7-day simple moving average of close price
        -- Requires exactly 7 non-NULL close prices in the window.
        CASE
            WHEN COUNT(wr.close) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) = 7
            THEN AVG(wr.close) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS sma_7,

        -- Metric 6: sma_30 -- 30-day simple moving average of close price
        -- Requires exactly 30 non-NULL close prices in the window.
        CASE
            WHEN COUNT(wr.close) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) = 30
            THEN AVG(wr.close) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS sma_30,

        -- Metric 7: volume_ratio_30d -- today's volume / avg of prior 30 days
        -- Window EXCLUDES current day (ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING).
        -- Requires exactly 30 non-NULL prior volume observations.
        -- NULLIF prevents division by zero if the average is somehow zero.
        CASE
            WHEN COUNT(wr.volume_usd) OVER (
                PARTITION BY wr.asset_id ORDER BY wr.date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) = 30
            THEN wr.volume_usd / NULLIF(
                AVG(wr.volume_usd) OVER (
                    PARTITION BY wr.asset_id ORDER BY wr.date
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                ), 0
            )
            ELSE NULL
        END AS volume_ratio_30d

    FROM with_returns wr
)

-- Final INSERT: write all computed metrics into daily_metrics.
INSERT INTO daily_metrics (
    asset_id,
    date,
    daily_return_pct,
    daily_range_pct,
    vol_7d,
    vol_30d,
    sma_7,
    sma_30,
    volume_ratio_30d
)
SELECT
    asset_id,
    date,
    daily_return_pct,
    daily_range_pct,
    vol_7d,
    vol_30d,
    sma_7,
    sma_30,
    volume_ratio_30d
FROM with_all_metrics
ORDER BY asset_id, date;
"""


# ---------------------------------------------------------------------------
# Metrics Computation
# ---------------------------------------------------------------------------
def compute_metrics(conn: psycopg2.extensions.connection) -> int:
    """Delete existing metrics and recompute all 7 metrics via SQL CTE chain.

    Uses DELETE + INSERT inside a single transaction for idempotency.
    Returns the number of rows inserted into daily_metrics.
    """
    with conn.cursor() as cur:
        # Step 1: Clear existing metrics (idempotency).
        logger.info("Deleting existing rows from daily_metrics...")
        cur.execute("DELETE FROM daily_metrics;")
        deleted: int = cur.rowcount
        logger.info("Deleted %d existing rows from daily_metrics.", deleted)

        # Step 2: Execute the CTE chain to compute and insert all metrics.
        logger.info("Computing all 7 metrics via SQL CTE chain...")
        cur.execute(METRICS_SQL)
        inserted: int = cur.rowcount
        logger.info("Inserted %d rows into daily_metrics.", inserted)

    # Commit the transaction (DELETE + INSERT are atomic).
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Validation Summary
# ---------------------------------------------------------------------------
def log_validation_summary(conn: psycopg2.extensions.connection) -> None:
    """Log a summary of NULL counts per asset to verify the NULL waterfall.

    Expected NULL counts per asset (from formula validation document):
        daily_return_pct: 1
        daily_range_pct:  0
        vol_7d:           7
        vol_30d:          30
        sma_7:            6
        sma_30:           29
        volume_ratio_30d: 30
    """
    validation_sql: str = """
        SELECT
            a.symbol,
            dm.asset_id,
            COUNT(*)                                    AS total_rows,
            COUNT(*) - COUNT(dm.daily_return_pct)       AS null_return,
            COUNT(*) - COUNT(dm.daily_range_pct)        AS null_range,
            COUNT(*) - COUNT(dm.vol_7d)                 AS null_vol7,
            COUNT(*) - COUNT(dm.vol_30d)                AS null_vol30,
            COUNT(*) - COUNT(dm.sma_7)                  AS null_sma7,
            COUNT(*) - COUNT(dm.sma_30)                 AS null_sma30,
            COUNT(*) - COUNT(dm.volume_ratio_30d)       AS null_volratio
        FROM daily_metrics dm
        JOIN assets a ON a.asset_id = dm.asset_id
        GROUP BY a.symbol, dm.asset_id
        ORDER BY dm.asset_id;
    """
    with conn.cursor() as cur:
        cur.execute(validation_sql)
        rows = cur.fetchall()

    logger.info("-" * 72)
    logger.info("NULL Waterfall Validation (expected: ret=1, rng=0, v7=7, v30=30, s7=6, s30=29, vr=30)")
    logger.info(
        "%-6s %5s %6s %6s %6s %6s %6s %6s %6s",
        "Symbol", "Total", "Ret", "Rng", "Vol7", "Vol30", "SMA7", "SMA30", "VolR",
    )
    logger.info("-" * 72)

    all_ok: bool = True
    for row in rows:
        symbol, asset_id, total, n_ret, n_rng, n_v7, n_v30, n_s7, n_s30, n_vr = row
        logger.info(
            "%-6s %5d %6d %6d %6d %6d %6d %6d %6d",
            symbol, total, n_ret, n_rng, n_v7, n_v30, n_s7, n_s30, n_vr,
        )
        # Check against expected values.
        if n_ret != 1:
            logger.warning("  %s: Expected 1 NULL for daily_return_pct, got %d", symbol, n_ret)
            all_ok = False
        if n_rng != 0:
            logger.warning("  %s: Expected 0 NULLs for daily_range_pct, got %d", symbol, n_rng)
            all_ok = False
        if n_v7 != 7:
            logger.warning("  %s: Expected 7 NULLs for vol_7d, got %d", symbol, n_v7)
            all_ok = False
        if n_v30 != 30:
            logger.warning("  %s: Expected 30 NULLs for vol_30d, got %d", symbol, n_v30)
            all_ok = False
        if n_s7 != 6:
            logger.warning("  %s: Expected 6 NULLs for sma_7, got %d", symbol, n_s7)
            all_ok = False
        if n_s30 != 29:
            logger.warning("  %s: Expected 29 NULLs for sma_30, got %d", symbol, n_s30)
            all_ok = False
        if n_vr != 30:
            logger.warning("  %s: Expected 30 NULLs for volume_ratio_30d, got %d", symbol, n_vr)
            all_ok = False

    if all_ok:
        logger.info("NULL waterfall validation PASSED for all assets.")
    else:
        logger.warning("NULL waterfall validation had WARNINGS -- review output above.")


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------
def main() -> None:
    """Entry point: connect to DB, compute all metrics, log results."""

    # Load .env if present (no error if missing).
    load_dotenv()

    logger.info("=" * 72)
    logger.info("Compute Daily Metrics Pipeline")
    logger.info("=" * 72)
    logger.info("Strategy: DELETE + INSERT (full recompute) in a single transaction")
    logger.info("Method  : SQL CTE chain with PostgreSQL window functions")
    logger.info(
        "Metrics : daily_return_pct, daily_range_pct, vol_7d, vol_30d, "
        "sma_7, sma_30, volume_ratio_30d"
    )

    # Connect to database.
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = get_db_connection()

        # Compute all metrics.
        t_start: float = time.perf_counter()
        rows_inserted: int = compute_metrics(conn)
        t_elapsed: float = time.perf_counter() - t_start

        logger.info("-" * 72)
        logger.info("Metrics computation complete.")
        logger.info("  Rows inserted : %d", rows_inserted)
        logger.info("  Time elapsed  : %.2f seconds", t_elapsed)

        # Run validation summary.
        log_validation_summary(conn)

        logger.info("=" * 72)
        logger.info("Pipeline finished successfully.")

    except Exception:
        logger.exception("Fatal error during metrics computation.")
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
