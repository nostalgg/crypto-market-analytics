"""
llm.py — Claude API functions for the SQL assistant.

Uses claude-3-5-haiku-20241022 for cost-effective SQL generation and
insight generation. Approximate cost per query:
  ~2000 input tokens + ~300 output tokens ≈ $0.001

Functions:
    nl_to_sql(question)                          → SQL string
    nl_to_sql_with_error(question, sql, error)   → corrected SQL string
    generate_insight(question, sql, summary)     → 2-4 sentence insight
"""

import os
import re

import anthropic
from dotenv import load_dotenv
import pathlib

from schema_context import SCHEMA_CONTEXT

# Load .env so ANTHROPIC_API_KEY is available when this module is imported.
_ENV_PATH = pathlib.Path(__file__).parent.parent / "config" / ".env"
load_dotenv(_ENV_PATH)

_MODEL = "claude-3-5-haiku-20241022"

# Build the Anthropic client once at module load (thread-safe for Streamlit).
_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# ---------------------------------------------------------------------------
# System prompt shared by nl_to_sql and nl_to_sql_with_error
# ---------------------------------------------------------------------------
_SQL_SYSTEM_PROMPT = f"""You are an expert PostgreSQL analyst for a crypto market data warehouse.
Your only job is to convert natural-language questions into valid PostgreSQL SELECT statements.

{SCHEMA_CONTEXT}

=== RULES (follow all 10 strictly) ===
1. Output ONLY the SQL statement — no markdown fences, no explanation, no preamble.
2. Every query must be a SELECT statement. Never use INSERT, UPDATE, DELETE, DROP, CREATE, or TRUNCATE.
3. Always add LIMIT (default 100, lower if the question clearly needs fewer rows).
4. Always JOIN the assets table when filtering by a specific crypto symbol (use a.symbol = 'XYZ').
5. Prefer daily_metrics for any pre-computed metric (daily_return_pct, vol_7d, vol_30d, sma_7, sma_30, volume_ratio_30d).
6. Use daily_prices for raw OHLCV data (open, high, low, close, volume_usd).
7. Never reference market_cap_usd — it is always NULL and will produce no useful results.
8. Add IS NOT NULL filters for any metric with a NULL warm-up period (daily_return_pct, vol_7d, vol_30d, sma_7, sma_30, volume_ratio_30d).
9. Use date_dim for period-based grouping (year, quarter, month) rather than EXTRACT() on the fly.
10. Cast NUMERIC columns to ::NUMERIC before ROUND() to avoid type errors, e.g. ROUND(avg_val::NUMERIC, 4).

=== FEW-SHOT EXAMPLES ===

--- Example 1: Single-asset time series ---
Question: Show BTC's 30-day rolling volatility for 2024
SQL:
SELECT
    a.symbol,
    dm.date,
    ROUND(dm.vol_30d::NUMERIC, 6) AS vol_30d
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
WHERE a.symbol = 'BTC'
  AND dm.date BETWEEN '2024-01-01' AND '2024-12-31'
  AND dm.vol_30d IS NOT NULL
ORDER BY dm.date
LIMIT 100;

--- Example 2: Cross-asset ranking ---
Question: Which asset had the highest average daily return in Q1 2023?
SQL:
SELECT
    a.symbol,
    ROUND(AVG(dm.daily_return_pct)::NUMERIC, 4) AS avg_daily_return_pct
FROM daily_metrics dm
JOIN assets a ON a.asset_id = dm.asset_id
JOIN date_dim d ON d.date_id = dm.date
WHERE d.year = 2023
  AND d.quarter = 1
  AND dm.daily_return_pct IS NOT NULL
GROUP BY a.symbol
ORDER BY avg_daily_return_pct DESC
LIMIT 10;

--- Example 3: Event-driven analysis ---
Question: Show ETH's closing price 14 days before and after the Merge
SQL:
SELECT
    a.symbol,
    dp.date,
    dp.close,
    CASE WHEN dp.date < me.event_date THEN 'before' ELSE 'after' END AS period,
    (dp.date - me.event_date) AS days_from_event
FROM market_events me
JOIN assets a ON a.symbol = 'ETH'
JOIN daily_prices dp
    ON dp.asset_id = a.asset_id
    AND dp.date BETWEEN me.event_date - 14 AND me.event_date + 14
WHERE me.title ILIKE '%Merge%'
ORDER BY dp.date
LIMIT 50;
"""


def _strip_sql_fences(text: str) -> str:
    """Remove ```sql ... ``` or ``` ... ``` markdown fences if present."""
    text = text.strip()
    # Remove opening fence (```sql or ```)
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    # Remove closing fence
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def nl_to_sql(question: str) -> str:
    """Translate a natural-language question into a PostgreSQL SELECT statement.

    Args:
        question: The user's natural-language question.

    Returns:
        A clean SQL string (no markdown fences).
    """
    response = _client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        temperature=0,
        system=_SQL_SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": question},
        ],
    )
    sql = response.content[0].text
    return _strip_sql_fences(sql)


def nl_to_sql_with_error(
    question: str,
    failed_sql: str,
    error_message: str,
) -> str:
    """Retry SQL generation after a query execution error.

    Uses a multi-turn conversation so the model sees exactly what it
    generated before and the error it caused.

    Args:
        question:      The original user question.
        failed_sql:    The SQL that caused the error.
        error_message: The error string returned by PostgreSQL.

    Returns:
        A corrected SQL string (no markdown fences).
    """
    response = _client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        temperature=0,
        system=_SQL_SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": question},
            {"role": "assistant", "content": failed_sql},
            {
                "role": "user",
                "content": (
                    f"That query failed with this PostgreSQL error:\n\n"
                    f"{error_message}\n\n"
                    "Please fix the SQL and return only the corrected query."
                ),
            },
        ],
    )
    sql = response.content[0].text
    return _strip_sql_fences(sql)


def generate_insight(
    question: str,
    sql: str,
    result_summary: str,
) -> str:
    """Generate a 2-4 sentence analytical insight from a query result.

    Args:
        question:       The original user question.
        sql:            The SQL that produced the result.
        result_summary: Compact text summary of the DataFrame (≤1200 chars).

    Returns:
        A 2-4 sentence insight string.
    """
    system = (
        "You are a crypto market analyst. Given a user question, the SQL query that "
        "answered it, and a summary of the query results, write a 2-4 sentence insight. "
        "Be specific about asset names, numbers, and timeframes mentioned in the data. "
        "Do not restate the question. Do not explain how the SQL works. "
        "Focus on what the data reveals about market behavior."
    )

    user_content = (
        f"Question: {question}\n\n"
        f"SQL used:\n{sql}\n\n"
        f"Query result summary:\n{result_summary}"
    )

    response = _client.messages.create(
        model=_MODEL,
        max_tokens=512,
        temperature=0.3,
        system=system,
        messages=[
            {"role": "user", "content": user_content},
        ],
    )
    return response.content[0].text.strip()
