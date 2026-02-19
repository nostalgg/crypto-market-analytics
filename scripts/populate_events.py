"""
Market Events Population Script for Crypto Market Analytics Data Warehouse.

Purpose:
    Populates the market_events table with 18 curated market events spanning
    the 2022-01-01 to 2025-10-31 analysis window. Events include crashes,
    halvings, regulatory actions, protocol upgrades, exchange collapses, and
    market milestones that are used for event-driven analysis in Phase 4.

Strategy:
    Uses TRUNCATE + INSERT to ensure idempotency. Since market_events has no
    natural unique constraint (the same date could theoretically have multiple
    events), a simple upsert is not possible. Instead, every run truncates the
    table (resetting the SERIAL sequence) and re-inserts all events from
    scratch. This guarantees a clean, reproducible state on every execution.

Usage:
    # Configure via .env file or environment variables:
    #   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

    python populate_events.py

Dependencies:
    psycopg2-binary  -- PostgreSQL adapter
    python-dotenv    -- Environment variable loading from .env files
"""

import logging
import os
import sys
from typing import Optional

import psycopg2
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
logger = logging.getLogger("populate_events")

# ---------------------------------------------------------------------------
# Market Events Data (18 events from Phase 1 Domain Validation)
# ---------------------------------------------------------------------------
MARKET_EVENTS: list[dict[str, Optional[str]]] = [
    {
        "event_date": "2022-05-09",
        "event_type": "crash",
        "title": "Terra/LUNA collapse -- UST stablecoin depeg triggers death spiral",
        "description": (
            "UST depegged from $1 starting May 7, accelerating on May 9. LUNA "
            "collapsed from $80 to near zero. Over $40B in value wiped out, "
            "triggering contagion across DeFi and centralized lenders. Massive "
            "volume spikes and correlation convergence across all assets."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2022-06-13",
        "event_type": "crash",
        "title": "Celsius freezes withdrawals -- crypto lending crisis begins",
        "description": (
            "Celsius Network froze all withdrawals, swaps, and transfers on "
            "June 12-13, signaling insolvency. This triggered cascading "
            "liquidations across DeFi protocols and amplified bear market "
            "selling pressure. Filed Chapter 11 on July 13."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2022-06-27",
        "event_type": "exchange_event",
        "title": "Three Arrows Capital ordered to liquidate",
        "description": (
            "BVI court ordered liquidation of 3AC, a $10B crypto hedge fund, "
            "after it failed margin calls following Terra/LUNA exposure. The "
            "collapse deepened the credit contagion, forcing liquidations of "
            "3AC positions across multiple protocols and exchanges."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2022-09-15",
        "event_type": "protocol_upgrade",
        "title": "Ethereum Merge -- transition from Proof of Work to Proof of Stake",
        "description": (
            "Ethereum completed its transition to PoS at block 15537393, "
            "reducing energy consumption by ~99.95%. Anticipated for years, "
            "the Merge was a sell-the-news event -- ETH declined ~15% in the "
            "following week despite successful execution. Provides a clean "
            "before/after analytical window for ETH volatility and correlation."
        ),
        "affected_assets": "ETH",
        "source_url": None,
    },
    {
        "event_date": "2022-11-02",
        "event_type": "exchange_event",
        "title": "CoinDesk exposes Alameda/FTX balance sheet -- FTX crisis begins",
        "description": (
            "CoinDesk published an investigation revealing Alameda Research "
            "held $3.66B in FTT tokens as collateral, exposing dangerous "
            "financial entanglement with FTX. This report triggered the chain "
            "of events leading to FTX collapse. Marks the start of the crisis "
            "period."
        ),
        "affected_assets": "ALL,SOL",
        "source_url": None,
    },
    {
        "event_date": "2022-11-08",
        "event_type": "crash",
        "title": "FTX halts withdrawals -- exchange collapse and $6B bank run",
        "description": (
            "After Binance announced selling $580M in FTT (Nov 6), FTX saw "
            "$6B in withdrawals over 72 hours. On Nov 8, FTX froze "
            "withdrawals. FTT crashed 80%. Binance offered then withdrew "
            "acquisition. FTX filed bankruptcy Nov 11. SOL crashed >50% due "
            "to Alameda holdings. Largest single-event drawdown for multiple "
            "assets in our dataset."
        ),
        "affected_assets": "ALL,SOL",
        "source_url": None,
    },
    {
        "event_date": "2023-03-10",
        "event_type": "macro_event",
        "title": "Silicon Valley Bank collapses -- US banking crisis hits crypto",
        "description": (
            "SVB was seized by FDIC on March 10. Circle disclosed $3.3B "
            "exposure, causing USDC to depeg to $0.87. After the Fed "
            "announced depositor backstop on March 12, BTC rallied 27% in 3 "
            "days as safe-haven narrative strengthened. Sharp reversal "
            "pattern: crash then rapid recovery."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2023-04-12",
        "event_type": "protocol_upgrade",
        "title": "Ethereum Shanghai/Shapella upgrade -- staking withdrawals enabled",
        "description": (
            "Shanghai upgrade activated at 22:27 UTC, enabling validators to "
            "withdraw staked ETH for the first time since December 2020. "
            "Despite fears of a sell-off, ETH rose 6% to $2,000. Over 4.4M "
            "ETH was subsequently deposited into staking, showing net "
            "positive demand. Tests sell-the-rumor/buy-the-news pattern."
        ),
        "affected_assets": "ETH",
        "source_url": None,
    },
    {
        "event_date": "2023-06-05",
        "event_type": "regulatory",
        "title": "SEC sues Binance and Coinbase in consecutive days",
        "description": (
            "SEC filed suit against Binance on June 5 and Coinbase on June 6, "
            "2023, alleging securities law violations. BNB dropped ~10% on "
            "the Binance news. Market briefly sold off but recovered within "
            "weeks as suits were seen as priced in. BNB shows distinct "
            "decorrelation during this event."
        ),
        "affected_assets": "BNB,ALL",
        "source_url": None,
    },
    {
        "event_date": "2023-10-24",
        "event_type": "market_milestone",
        "title": "BTC spot ETF approval anticipation rally -- BlackRock iShares filing progress",
        "description": (
            "BTC broke above $35,000 for the first time since May 2022, "
            "driven by news of BlackRock iShares Bitcoin Trust appearing on "
            "DTCC clearing lists. This marked the beginning of the "
            "ETF-anticipation rally that dominated Q4 2023. Volume surged "
            "well above 30-day averages across major assets."
        ),
        "affected_assets": "BTC",
        "source_url": None,
    },
    {
        "event_date": "2024-01-10",
        "event_type": "market_milestone",
        "title": "SEC approves 11 spot Bitcoin ETFs -- institutional access begins",
        "description": (
            "SEC approved spot BTC ETFs from BlackRock, Fidelity, and 9 other "
            "issuers. Trading began Jan 11. Paradoxically, BTC initially "
            "dropped from $49K to $42K over the following 2 weeks (classic "
            "sell-the-news), before beginning a sustained rally. Volume "
            "spiked 3-4x above 30-day averages."
        ),
        "affected_assets": "BTC,ALL",
        "source_url": None,
    },
    {
        "event_date": "2024-03-14",
        "event_type": "market_milestone",
        "title": "Bitcoin hits pre-halving ATH of $73,800 -- first ATH before a halving",
        "description": (
            "BTC reached $73,800 on March 14, 2024, setting a new all-time "
            "high before the halving for the first time in history. ETF "
            "inflows (particularly IBIT) drove institutional buying pressure. "
            "This broke the historical pattern where ATHs only occurred 12-18 "
            "months post-halving."
        ),
        "affected_assets": "BTC",
        "source_url": None,
    },
    {
        "event_date": "2024-04-20",
        "event_type": "halving",
        "title": "Fourth Bitcoin halving -- block reward reduced to 3.125 BTC",
        "description": (
            "Block reward halved from 6.25 to 3.125 BTC at block 840,000 on "
            "April 19-20, 2024. Unlike previous halvings, BTC had already set "
            "an ATH beforehand due to ETF demand. Post-halving price action "
            "was muted initially, then began trending higher. Provides "
            "comparative analysis opportunity vs. historical halving patterns."
        ),
        "affected_assets": "BTC",
        "source_url": None,
    },
    {
        "event_date": "2024-07-23",
        "event_type": "market_milestone",
        "title": "Spot Ethereum ETFs begin trading in the US",
        "description": (
            "SEC approved final S-1 filings on July 22; eight spot ETH ETFs "
            "began trading July 23 on Nasdaq, NYSE, and CBOE. Initial flows "
            "were modest compared to BTC ETFs. ETH showed muted reaction, "
            "having already priced in the May 23 initial 19b-4 approval. "
            "Useful comparison to BTC ETF launch dynamics."
        ),
        "affected_assets": "ETH",
        "source_url": None,
    },
    {
        "event_date": "2024-11-06",
        "event_type": "market_milestone",
        "title": "Trump wins 2024 presidential election -- crypto-friendly policy expected",
        "description": (
            "Trump won the presidential election on November 5 (results "
            "confirmed Nov 6). BTC surged from $69K on election night to over "
            "$75K within hours, then continued rallying. Market priced in "
            "expected pro-crypto regulatory environment. Triggered a sustained "
            "momentum streak across all crypto assets."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2024-12-05",
        "event_type": "market_milestone",
        "title": "Bitcoin crosses $100,000 for the first time",
        "description": (
            "BTC broke the psychologically significant $100K barrier on "
            "December 5, 2024, reaching $103,679. Driven by post-election "
            "momentum, ETF inflows, and halving supply dynamics. Reached "
            "$108,135 by December 17. This milestone attracted mainstream "
            "media coverage and new retail participation, visible in volume "
            "data."
        ),
        "affected_assets": "BTC,ALL",
        "source_url": None,
    },
    {
        "event_date": "2025-01-23",
        "event_type": "regulatory",
        "title": "Trump signs crypto executive order -- Strategic Bitcoin Reserve proposed",
        "description": (
            'President Trump signed "Strengthening American Leadership in '
            'Digital Financial Technology" executive order. Key provisions: '
            "creation of a Strategic Bitcoin Reserve working group, digital "
            "asset stockpile, crypto-friendly banking access, and ban on "
            "federal CBDC development. SEC also rescinded SAB 121, removing "
            "major barrier for institutional custody. Broadly bullish signal."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
    {
        "event_date": "2025-10-10",
        "event_type": "crash",
        "title": "Trump 100% China tariff threat triggers $19B crypto liquidation cascade",
        "description": (
            "Trump announced 100% tariffs on Chinese imports on October 10. "
            "BTC fell from $122K to $104K (-15%). SOL crashed 40%. Over "
            "$19.1B in leveraged positions liquidated in 24 hours -- the "
            "largest single-day liquidation event in crypto history. Altcoins "
            "dropped 20-40%. This ended the 2024-2025 bull market and "
            "initiated a new drawdown cycle."
        ),
        "affected_assets": "ALL",
        "source_url": None,
    },
]


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
# Populate Market Events
# ---------------------------------------------------------------------------
def populate_market_events(
    conn: psycopg2.extensions.connection,
    events: list[dict[str, Optional[str]]],
) -> int:
    """Truncate the market_events table and insert all events.

    Uses TRUNCATE + INSERT for idempotency. Since market_events has no
    natural unique constraint (multiple events could share the same date),
    a simple ON CONFLICT upsert is not feasible. TRUNCATE resets the
    SERIAL sequence and removes all existing rows, then all events are
    re-inserted cleanly.

    Returns the number of rows inserted.
    """
    truncate_sql = "TRUNCATE TABLE market_events RESTART IDENTITY;"
    insert_sql = """
        INSERT INTO market_events
            (event_date, event_type, title, description, affected_assets, source_url)
        VALUES
            (%s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        # Truncate existing events and reset serial counter.
        cur.execute(truncate_sql)
        logger.info(
            "Truncated market_events table (RESTART IDENTITY). "
            "All previous rows removed."
        )

        # Insert all events.
        inserted = 0
        for event in events:
            cur.execute(insert_sql, (
                event["event_date"],
                event["event_type"],
                event["title"],
                event["description"],
                event["affected_assets"],
                event["source_url"],
            ))
            inserted += 1

    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Entry point: connect to DB, truncate market_events, insert all events."""

    # Load .env if present (no error if missing).
    load_dotenv()

    logger.info("=" * 72)
    logger.info("Market Events Population Script")
    logger.info("=" * 72)
    logger.info(
        "Strategy: TRUNCATE + INSERT (idempotent -- safe to re-run). "
        "All existing market_events rows will be replaced."
    )
    logger.info("Events to insert: %d", len(MARKET_EVENTS))

    # Connect to database.
    conn = get_db_connection()

    try:
        inserted = populate_market_events(conn, MARKET_EVENTS)
        logger.info("-" * 72)
        logger.info(
            "Done. Successfully inserted %d market events into market_events table.",
            inserted,
        )
    except Exception:
        logger.exception("Fatal error during market events population.")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
