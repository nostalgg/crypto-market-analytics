"""
db.py — PostgreSQL connection and query execution for the LLM assistant.

Mirrors the connection pattern from SQL/scripts/ingest_yahoo.py.
dotenv is loaded from SQL/config/.env using an absolute path so the
module works regardless of the working directory from which Streamlit
is launched.

DB user should ideally be read-only (GRANT SELECT ON ALL TABLES IN
SCHEMA public TO <user>;) since this module only issues SELECT statements.
"""

import pathlib
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load .env from SQL/config/.env — absolute path, cwd-independent.
_ENV_PATH = pathlib.Path(__file__).parent.parent / "config" / ".env"
load_dotenv(_ENV_PATH)


def get_connection() -> psycopg2.extensions.connection:
    """Open and return a fresh psycopg2 connection.

    Opens one connection per call (thread-safe for Streamlit's execution
    model). autocommit=True is appropriate because this app issues only
    SELECT statements and we want to avoid idle transactions.

    Raises psycopg2.OperationalError if the connection cannot be established.
    """
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    conn.autocommit = True
    return conn


def run_query(
    sql: str,
    timeout_ms: int = 15000,
) -> tuple[pd.DataFrame, Optional[str]]:
    """Execute a SELECT query and return (DataFrame, None) or (empty_df, error).

    Args:
        sql:        The SQL string to execute. Must start with SELECT.
        timeout_ms: Statement timeout in milliseconds (default 15 s).
                    Prevents runaway queries from blocking the UI.

    Returns:
        (df, None)        on success — df contains the query result.
        (empty_df, msg)   on failure — msg is the human-readable error string.
    """
    # Safety guard: only allow SELECT statements.
    if not sql.strip().upper().startswith("SELECT"):
        return pd.DataFrame(), "Only SELECT statements are allowed."

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Apply server-side timeout to prevent runaway queries.
            cur.execute(f"SET statement_timeout = {timeout_ms}")
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
        df = pd.DataFrame(rows, columns=columns)
        return df, None

    except Exception as exc:
        return pd.DataFrame(), str(exc)

    finally:
        if conn is not None:
            conn.close()


def get_sample_questions() -> list[str]:
    """Return 6 hard-coded example questions for the sidebar.

    These are designed to exercise different query patterns:
    time-series, cross-asset ranking, volatility, events, and
    volume anomaly detection.
    """
    return [
        "Show BTC's 30-day rolling volatility for all of 2024",
        "Which asset had the highest average daily return in Q1 2023?",
        "Show ETH's closing price 14 days before and after the Merge",
        "Compare average vol_30d for BTC and ETH in 2022 vs 2024",
        "Which days had the largest volume spikes (volume_ratio_30d > 3) for SOL?",
        "Show the top 5 worst single-day returns across all assets",
    ]
