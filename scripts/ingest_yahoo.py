"""
Yahoo Finance Data Ingestion Script for Crypto Market Analytics Data Warehouse.

Purpose:
    Fetches historical daily OHLCV market data from Yahoo Finance for a
    configured set of crypto assets and loads it into a PostgreSQL data
    warehouse. Also populates the assets dimension table and date_dim
    calendar dimension.

    Yahoo Finance provides full Open/High/Low/Close/Volume data for crypto
    pairs (e.g., BTC-USD), unlike CoinGecko's free tier which only returns
    close prices. This enables intraday range analysis, Parkinson/Garman-Klass
    volatility estimators, and candlestick-based metrics.

    Trade-off: Yahoo Finance does not provide market capitalization data.
    The market_cap_usd column in daily_prices will be NULL.

Usage:
    # Configure via .env file or environment variables:
    #   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    #   START_DATE (default: 2022-01-01)
    #   END_DATE (default: 2025-10-31)

    python ingest_yahoo.py

Dependencies:
    psycopg2-binary  -- PostgreSQL adapter
    yfinance         -- Yahoo Finance API wrapper
    python-dotenv    -- Environment variable loading from .env files
"""

import logging
import os
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import yfinance as yf
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
logger = logging.getLogger("ingest_yahoo")

# ---------------------------------------------------------------------------
# Asset Configuration
# ---------------------------------------------------------------------------
# Each asset maps our internal symbol to the Yahoo Finance ticker.
# Yahoo Finance uses {SYMBOL}-USD for crypto pairs.
ASSETS: list[dict[str, str]] = [
    {"symbol": "BTC",  "name": "Bitcoin",   "yahoo_ticker": "BTC-USD",  "category": "layer1"},
    {"symbol": "ETH",  "name": "Ethereum",  "yahoo_ticker": "ETH-USD",  "category": "layer1"},
    {"symbol": "SOL",  "name": "Solana",    "yahoo_ticker": "SOL-USD",  "category": "layer1"},
    {"symbol": "BNB",  "name": "BNB",       "yahoo_ticker": "BNB-USD",  "category": "layer1_exchange"},
    {"symbol": "ADA",  "name": "Cardano",   "yahoo_ticker": "ADA-USD",  "category": "layer1"},
    {"symbol": "AVAX", "name": "Avalanche", "yahoo_ticker": "AVAX-USD", "category": "layer1"},
    {"symbol": "LINK", "name": "Chainlink", "yahoo_ticker": "LINK-USD", "category": "oracle"},
    {"symbol": "DOT",  "name": "Polkadot",  "yahoo_ticker": "DOT-USD",  "category": "layer1_interop"},
]


# ---------------------------------------------------------------------------
# Decimal Conversion
# ---------------------------------------------------------------------------
def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert a numeric value to Decimal via str() intermediate to avoid
    float-to-Decimal precision artefacts.

    Returns None for NaN/None values (which yfinance can produce for
    days with missing data).
    """
    if value is None:
        return None
    try:
        s = str(value)
        if s in ("nan", "NaN", "inf", "-inf", "None"):
            return None
        return Decimal(s)
    except (InvalidOperation, TypeError, ValueError):
        logger.warning("Could not convert value to Decimal: %r", value)
        return None


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
# Populate Assets Dimension
# ---------------------------------------------------------------------------
def populate_assets(
    conn: psycopg2.extensions.connection,
    assets: list[dict[str, str]],
) -> dict[str, int]:
    """Insert or update asset rows and return a mapping of symbol -> asset_id.

    Uses INSERT ... ON CONFLICT (symbol) DO UPDATE to ensure idempotency.
    Returns a dict like ``{"BTC": 1, "ETH": 2, ...}``.
    """
    upsert_sql = """
        INSERT INTO assets (symbol, name, category)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE
            SET name     = EXCLUDED.name,
                category = EXCLUDED.category
        RETURNING asset_id, symbol;
    """
    symbol_to_id: dict[str, int] = {}
    with conn.cursor() as cur:
        for asset in assets:
            cur.execute(upsert_sql, (asset["symbol"], asset["name"], asset["category"]))
            row = cur.fetchone()
            if row:
                symbol_to_id[row[1]] = row[0]
    conn.commit()
    logger.info("Upserted %d assets into assets table.", len(symbol_to_id))
    return symbol_to_id


# ---------------------------------------------------------------------------
# Populate Date Dimension
# ---------------------------------------------------------------------------
def populate_date_dim(
    conn: psycopg2.extensions.connection,
    start_date: date,
    end_date: date,
) -> int:
    """Generate all calendar dates in [start_date, end_date] and insert into date_dim.

    Computes year, quarter, month, ISO week number, day_of_week (0=Monday),
    and is_weekend flag for each date.  Uses INSERT ... ON CONFLICT DO NOTHING
    so the function is idempotent.

    Returns the number of *new* rows inserted.
    """
    insert_sql = """
        INSERT INTO date_dim (date_id, year, quarter, month, week, day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING;
    """
    rows: list[tuple[date, int, int, int, int, int, bool]] = []
    current = start_date
    while current <= end_date:
        iso_year, iso_week, iso_weekday = current.isocalendar()
        day_of_week = current.weekday()  # 0=Monday, 6=Sunday
        quarter = (current.month - 1) // 3 + 1
        is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6
        rows.append((
            current,
            current.year,
            quarter,
            current.month,
            iso_week,
            day_of_week,
            is_weekend,
        ))
        current += timedelta(days=1)

    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(insert_sql, row)
            inserted += cur.rowcount
    conn.commit()
    logger.info(
        "Date dimension: %d total dates in range, %d newly inserted.",
        len(rows),
        inserted,
    )
    return inserted


# ---------------------------------------------------------------------------
# Yahoo Finance Data Fetching
# ---------------------------------------------------------------------------
def fetch_yahoo_data(
    yahoo_ticker: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Fetch daily OHLCV data from Yahoo Finance for a single crypto asset.

    Uses the ``yfinance`` library which wraps Yahoo Finance's public API.
    Returns daily granularity with full Open/High/Low/Close/Volume data.

    No API key is required. No rate limiting is needed for the number of
    assets in this project (8 assets = 8 calls).

    Yahoo Finance's end date is exclusive, so we add 1 day to include the
    end_date in results.

    Returns a list of dicts with keys:
        date, open, high, low, close, volume_usd
    All monetary values are Decimal. volume_usd is the USD-equivalent
    volume reported by Yahoo Finance for crypto pairs.
    """
    # yfinance end date is exclusive — add 1 day to include end_date.
    yf_end = end_date + timedelta(days=1)

    logger.info(
        "  Downloading %s from Yahoo Finance (%s to %s)...",
        yahoo_ticker,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    try:
        ticker = yf.Ticker(yahoo_ticker)
        df = ticker.history(
            start=start_date.isoformat(),
            end=yf_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
    except Exception as exc:
        logger.error("  Failed to download %s: %s", yahoo_ticker, exc)
        return []

    if df is None or df.empty:
        logger.warning("  %s: No data returned from Yahoo Finance.", yahoo_ticker)
        return []

    records: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        # idx is a pandas Timestamp — convert to date.
        row_date = idx.date() if hasattr(idx, "date") else idx

        open_val = _to_decimal(row.get("Open"))
        high_val = _to_decimal(row.get("High"))
        low_val = _to_decimal(row.get("Low"))
        close_val = _to_decimal(row.get("Close"))
        volume_val = _to_decimal(row.get("Volume"))

        # Skip rows where close is missing or zero (invalid data).
        if close_val is None or close_val <= 0:
            logger.debug("  Skipping %s %s: invalid close=%s", yahoo_ticker, row_date, close_val)
            continue

        records.append({
            "date": row_date,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume_usd": volume_val,
        })

    logger.info("  %s: %d daily OHLCV records fetched.", yahoo_ticker, len(records))
    return records


# ---------------------------------------------------------------------------
# Populate Daily Prices Fact Table
# ---------------------------------------------------------------------------
def populate_daily_prices(
    conn: psycopg2.extensions.connection,
    asset_id: int,
    daily_data: list[dict[str, Any]],
) -> int:
    """Insert or update daily price records for a single asset.

    Uses INSERT ... ON CONFLICT (asset_id, date) DO UPDATE to achieve
    idempotent upserts.  All numeric columns are stored as Decimal.

    Yahoo Finance provides full OHLCV data. market_cap_usd is set to NULL
    as Yahoo Finance does not provide market capitalization.

    Returns the number of rows upserted.
    """
    upsert_sql = """
        INSERT INTO daily_prices
            (asset_id, date, open, high, low, close, volume_usd, market_cap_usd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asset_id, date) DO UPDATE
            SET open           = EXCLUDED.open,
                high           = EXCLUDED.high,
                low            = EXCLUDED.low,
                close          = EXCLUDED.close,
                volume_usd     = EXCLUDED.volume_usd,
                market_cap_usd = EXCLUDED.market_cap_usd;
    """
    total = 0
    with conn.cursor() as cur:
        for rec in daily_data:
            cur.execute(upsert_sql, (
                asset_id,
                rec["date"],
                rec["open"],            # Decimal — full OHLC from Yahoo
                rec["high"],            # Decimal
                rec["low"],             # Decimal
                rec["close"],           # Decimal
                rec["volume_usd"],      # Decimal
                None,                   # market_cap_usd (not available from Yahoo)
            ))
            total += 1

    conn.commit()
    logger.info("  asset_id=%d: upserted %d daily_prices rows.", asset_id, total)
    return total


# ---------------------------------------------------------------------------
# Gap Detection
# ---------------------------------------------------------------------------
def _detect_gaps(
    daily_data: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    symbol: str,
) -> None:
    """Log warnings for any expected dates that are missing from the fetched data.

    This is informational only -- gaps are not treated as errors because
    crypto data from Yahoo Finance may occasionally have minor gaps.
    """
    if not daily_data:
        logger.warning("  %s: No data at all in range %s to %s.", symbol, start_date, end_date)
        return

    actual_dates = {rec["date"] for rec in daily_data}
    expected: set[date] = set()
    current = max(start_date, daily_data[0]["date"])
    last = min(end_date, daily_data[-1]["date"])
    while current <= last:
        expected.add(current)
        current += timedelta(days=1)

    missing = sorted(expected - actual_dates)
    if missing:
        logger.warning(
            "  %s: %d gap(s) detected in date range. First 10: %s",
            symbol,
            len(missing),
            [d.isoformat() for d in missing[:10]],
        )
    else:
        logger.info("  %s: No date gaps detected.", symbol)


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    """Entry point: connect to DB, populate dimensions, fetch and load prices."""

    # Load .env if present (no error if missing).
    load_dotenv()

    # Configuration from environment.
    start_str = os.environ.get("START_DATE", "2022-01-01")
    end_str = os.environ.get("END_DATE", "2025-10-31")
    start_date = date.fromisoformat(start_str)
    end_date = date.fromisoformat(end_str)

    logger.info("=" * 72)
    logger.info("Yahoo Finance Ingestion Pipeline")
    logger.info("=" * 72)
    logger.info("Date range : %s to %s", start_date.isoformat(), end_date.isoformat())
    logger.info("Data source: Yahoo Finance (yfinance) — full OHLCV, no API key required")
    logger.info("Assets     : %s", ", ".join(a["symbol"] for a in ASSETS))
    logger.info(
        "Note       : market_cap_usd will be NULL (not available from Yahoo Finance). "
        "None of the 6 core business questions require market cap."
    )

    # Connect to database.
    conn = get_db_connection()

    try:
        # 1. Populate assets dimension.
        logger.info("-" * 72)
        logger.info("Step 1: Populating assets dimension...")
        symbol_to_id = populate_assets(conn, ASSETS)
        for sym, aid in sorted(symbol_to_id.items(), key=lambda x: x[1]):
            logger.info("  %s -> asset_id %d", sym, aid)

        # 2. Populate date dimension.
        logger.info("-" * 72)
        logger.info("Step 2: Populating date dimension...")
        populate_date_dim(conn, start_date, end_date)

        # 3. Fetch and load daily prices for each asset.
        logger.info("-" * 72)
        logger.info("Step 3: Fetching and loading daily OHLCV prices...")

        grand_total = 0
        for asset_cfg in ASSETS:
            symbol = asset_cfg["symbol"]
            yahoo_ticker = asset_cfg["yahoo_ticker"]
            asset_id = symbol_to_id.get(symbol)
            if asset_id is None:
                logger.error("No asset_id found for %s -- skipping.", symbol)
                continue

            logger.info("Fetching %s (ticker=%s)...", symbol, yahoo_ticker)

            daily_data = fetch_yahoo_data(
                yahoo_ticker=yahoo_ticker,
                start_date=start_date,
                end_date=end_date,
            )

            if not daily_data:
                logger.warning("  %s: No data returned. Skipping DB insert.", symbol)
                continue

            # Detect gaps.
            _detect_gaps(daily_data, start_date, end_date, symbol)

            # Insert into daily_prices.
            rows_upserted = populate_daily_prices(conn, asset_id, daily_data)
            grand_total += rows_upserted

        logger.info("-" * 72)
        logger.info("Ingestion complete. Total rows upserted: %d", grand_total)

    except Exception:
        logger.exception("Fatal error during ingestion.")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
