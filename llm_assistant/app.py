"""
app.py — Streamlit UI for the Crypto Analytics LLM Assistant.

Launch with:
    cd SQL/llm_assistant
    streamlit run app.py

The app allows users to ask natural-language questions about the
crypto_analytics PostgreSQL warehouse. For each question it:
  1. Generates SQL via Claude Haiku (nl_to_sql)
  2. Runs the query (run_query)
  3. Optionally retries once on error (nl_to_sql_with_error)
  4. Auto-detects and renders a chart (line or bar)
  5. Generates a 2-4 sentence insight (generate_insight)
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from db import get_sample_questions, run_query
from llm import generate_insight, nl_to_sql, nl_to_sql_with_error

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Crypto Analytics Assistant",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
if "query_history" not in st.session_state:
    st.session_state.query_history = []  # list of dicts


# ---------------------------------------------------------------------------
# Helper: DataFrame → compact text for the insight prompt
# ---------------------------------------------------------------------------
def df_to_summary(df: pd.DataFrame, max_chars: int = 1200) -> str:
    """Convert a DataFrame to a compact text representation (≤ max_chars).

    Includes shape, column dtypes, and the first 15 rows as a pipe-
    delimited table — enough context for the model to write a specific
    insight without exceeding the token budget.
    """
    lines = [
        f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
        f"Columns: {', '.join(f'{c} ({df[c].dtype})' for c in df.columns)}",
        "",
        "First rows (pipe-delimited):",
        " | ".join(df.columns),
        " | ".join(["---"] * len(df.columns)),
    ]
    for _, row in df.head(15).iterrows():
        lines.append(" | ".join(str(v) for v in row))

    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n[truncated]"
    return text


# ---------------------------------------------------------------------------
# Helper: auto-detect and render a Plotly chart
# ---------------------------------------------------------------------------
def maybe_chart(df: pd.DataFrame):
    """Render a line or bar chart if the DataFrame shape supports it.

    Rules:
      - 1 date/datetime column + ≥1 numeric column → line chart (time series)
      - 1 categorical column + 1 numeric column + ≤20 rows → bar chart (ranking)
      - Otherwise: no chart is rendered.

    Returns True if a chart was rendered, False otherwise.
    """
    if df.empty or len(df.columns) < 2:
        return False

    date_cols = [
        c for c in df.columns
        if pd.api.types.is_datetime64_any_dtype(df[c])
        or (df[c].dtype == object and _looks_like_date(df[c]))
    ]
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical_cols = [
        c for c in df.columns
        if c not in date_cols and c not in numeric_cols
    ]

    # --- Time-series line chart ---
    if len(date_cols) == 1 and len(numeric_cols) >= 1:
        date_col = date_cols[0]
        # Coerce to datetime if stored as object
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).sort_values(date_col)

        # If there's a symbol/categorical column, use it as color
        color_col = categorical_cols[0] if categorical_cols else None
        fig = px.line(
            df,
            x=date_col,
            y=numeric_cols,
            color=color_col,
            title="Time Series",
        )
        st.plotly_chart(fig, use_container_width=True)
        return True

    # --- Ranking bar chart (≤20 rows) ---
    if (
        len(categorical_cols) >= 1
        and len(numeric_cols) == 1
        and len(df) <= 20
    ):
        cat_col = categorical_cols[0]
        num_col = numeric_cols[0]
        fig = px.bar(
            df.sort_values(num_col, ascending=False),
            x=cat_col,
            y=num_col,
            title="Ranking",
        )
        st.plotly_chart(fig, use_container_width=True)
        return True

    return False


def _looks_like_date(series: pd.Series) -> bool:
    """Heuristic: check if an object column looks like ISO dates."""
    sample = series.dropna().head(5)
    if sample.empty:
        return False
    try:
        pd.to_datetime(sample, errors="raise")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Core execution flow: generate SQL → run → (retry on error) → insight
# ---------------------------------------------------------------------------
def execute_with_retry(question: str) -> dict:
    """Run the full LLM + DB pipeline for a question.

    Returns a dict with keys:
        question, sql, df, insight, error, retried, retry_sql, retry_error
    """
    result = {
        "question": question,
        "sql": "",
        "df": pd.DataFrame(),
        "insight": "",
        "error": None,
        "retried": False,
        "retry_sql": "",
        "retry_error": None,
    }

    # Step 1: Generate SQL
    with st.spinner("Generating SQL…"):
        sql = nl_to_sql(question)
    result["sql"] = sql

    # Step 2: Run query
    with st.spinner("Running query…"):
        df, err = run_query(sql)

    if err is None:
        result["df"] = df
        # Step 3: Generate insight
        with st.spinner("Generating insight…"):
            summary = df_to_summary(df)
            result["insight"] = generate_insight(question, sql, summary)
        return result

    # Step 4: First attempt failed — retry with error context
    result["error"] = err
    result["retried"] = True

    with st.spinner("Query failed — retrying with error context…"):
        retry_sql = nl_to_sql_with_error(question, sql, err)
    result["retry_sql"] = retry_sql

    with st.spinner("Running corrected query…"):
        df2, err2 = run_query(retry_sql)

    if err2 is None:
        result["df"] = df2
        result["sql"] = retry_sql  # Show the working SQL prominently
        with st.spinner("Generating insight…"):
            summary = df_to_summary(df2)
            result["insight"] = generate_insight(question, retry_sql, summary)
    else:
        result["retry_error"] = err2

    return result


# ---------------------------------------------------------------------------
# Render a single result dict into the main area
# ---------------------------------------------------------------------------
def render_result(res: dict):
    """Display the result of execute_with_retry() in the main area."""
    if res["retried"] and res["retry_error"] is None:
        st.info(
            "The first query attempt failed. The SQL was automatically corrected "
            "and the retry succeeded."
        )

    # Generated SQL
    with st.expander("Generated SQL", expanded=True):
        st.code(res["sql"], language="sql")
        if res["retried"] and res["retry_error"] is not None:
            st.markdown("**First attempt (failed):**")
            st.code(res["sql"], language="sql")  # already the first sql
            st.error(f"Error: {res['error']}")
            st.markdown("**Retry attempt (also failed):**")
            st.code(res["retry_sql"], language="sql")
            st.error(f"Retry error: {res['retry_error']}")
            return  # Nothing more to show

    # Data table
    df = res["df"]
    if not df.empty:
        st.dataframe(df, use_container_width=True)

        # Chart
        with st.expander("Chart", expanded=True):
            rendered = maybe_chart(df)
            if not rendered:
                st.caption("No chart available for this result shape.")

        # AI Insight
        if res["insight"]:
            with st.expander("AI Insight", expanded=True):
                st.markdown(res["insight"])
    else:
        st.warning("The query returned no rows.")

    # Surface non-fatal first-attempt error even when retry succeeded
    if res["retried"] and res["retry_error"] is None and res["error"]:
        with st.expander("First attempt error (corrected automatically)"):
            st.error(res["error"])
            st.code(res.get("sql", ""), language="sql")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Query History")
    if not st.session_state.query_history:
        st.caption("No queries yet.")
    else:
        for i, h in enumerate(reversed(st.session_state.query_history)):
            label = h["question"][:60] + ("…" if len(h["question"]) > 60 else "")
            if st.button(label, key=f"hist_{i}"):
                # Re-render the stored result without re-running the query
                st.session_state.selected_history = h

    st.divider()
    st.subheader("Try these questions:")
    for idx, sample in enumerate(get_sample_questions()):
        if st.button(sample, key=f"sample_{idx}"):
            st.session_state.prefill_question = sample

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
st.title("Crypto Analytics Assistant")
st.caption(
    "Ask questions about BTC, ETH, SOL, BNB, ADA, AVAX, LINK, DOT "
    "— data from 2022-01-01 to 2025-10-31."
)

# Pre-fill text area if a sample question was clicked
default_question = st.session_state.pop("prefill_question", "")

question = st.text_area(
    "Your question:",
    value=default_question,
    height=80,
    placeholder="e.g. Which asset had the highest volatility in 2023?",
)

run_clicked = st.button("Run Query", type="primary")

# If a history item was selected from the sidebar, show it without re-running
if "selected_history" in st.session_state:
    render_result(st.session_state.pop("selected_history"))

elif run_clicked and question.strip():
    res = execute_with_retry(question.strip())
    # Prepend to history (most recent first)
    st.session_state.query_history.append(res)
    render_result(res)

elif run_clicked and not question.strip():
    st.warning("Please enter a question before clicking Run Query.")
