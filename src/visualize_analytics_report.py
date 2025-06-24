"""
📊 Module: visualize_analytics_report.py

Overview:
---------
This Streamlit app visualizes the performance and structure of the EQX Equal-Weighted Index,
a custom index of the top 100 US stocks by market capitalization.

It pulls data from the DuckDB database and presents insights through interactive charts,
performance metrics, and rebalancing analytics.

Visualizations & Features:
--------------------------
- Index Value Over Time (Line Chart)
- Index vs SPY Comparison (Line Chart)
- Normalized Performance (Base 100)
- Cumulative Return Plot
- Daily Return Comparison
- Histogram of Daily Returns
- Rolling Volatility (7-day)
- Drawdown Curve
- Best and Worst Day Highlights

Exploration Tools:
------------------
- Ticker breakdown by date
- Rebalancing comparison between two selected dates

Summary Dashboard:
------------------
- Displays final performance and risk metrics stored in `summary_metrics` table.

Dependencies:
-------------
- Streamlit, pandas, plotly, duckdb, numpy
- Config class from config.py for DB file path

Usage:
------
Run the dashboard with:
    streamlit run visualize_analytics_report.py

Ensure the index pipeline (data ingestion → index builder → metrics) has run before using this dashboard.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import numpy as np
from pathlib import Path
from config import Config


# ----------------------------
# Load Index Data
# ----------------------------
@st.cache_data(show_spinner=False)
def load_index_data():
    try:
        conn = duckdb.connect(Config.DUCKDB_FILE)
        df = conn.execute("SELECT * FROM index_metrics ORDER BY date").fetchdf()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to load index data: {e}")
        return pd.DataFrame()


# ----------------------------
# Load Summary Metrics
# ----------------------------
@st.cache_data(show_spinner=False)
def load_summary_metrics():
    try:
        conn = duckdb.connect(Config.DUCKDB_FILE)
        df = conn.execute("SELECT * FROM summary_metrics").fetchdf()
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Summary metrics load failed: {e}")
        return pd.DataFrame()


# ----------------------------
# List Tables
# ----------------------------
def list_tables():
    try:
        conn = duckdb.connect(Config.DUCKDB_FILE)
        tables = conn.execute("SHOW TABLES").fetchall()
        conn.close()
        return [t[0] for t in tables]
    except:
        return []


# ----------------------------
# Streamlit Layout
# ----------------------------
st.set_page_config(page_title="EQX Equal Index Dashboard", layout="wide")
st.title("📊 EQX Equal Index Dashboard")
st.markdown("Tracking your equal-weighted top 100 US stocks index")

with st.expander("Available Tables in DuckDB"):
    st.write(list_tables())

df = load_index_data()
summary_df = load_summary_metrics()

if df.empty:
    st.warning("No index data found. Please run your index builder pipeline first.")
    st.stop()

# --- Normalized Performance Calculation (Base = 100) ---
df["eqx_base_100"] = (df["index_value"] / df["index_value"].iloc[0]) * 100
df["spy_base_100"] = (df["spy_close"] / df["spy_close"].iloc[0]) * 100

# --- Total Return Metrics ---
eqx_return = df["eqx_base_100"].iloc[-1] - 100
spy_return = df["spy_base_100"].iloc[-1] - 100

tab1, tab2, tab3 = st.tabs(["📈 Performance", "🔍 Explore", "📊 Summary"])

# ----------------------------
# Tab 1 - Performance
# ----------------------------
with tab1:
    st.subheader("EQX Equal Index Value Over Time")
    fig = px.line(
        df, x="date", y="index_value", title="EQX Equal Index Performance", markers=True
    )
    fig.update_traces(line=dict(color="#FF69B4", width=2))
    fig.update_layout(template="simple_white")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Index vs SPY")
    comp_fig = px.line(
        df,
        x="date",
        y=["index_value", "spy_close"],
        labels={"value": "Value", "variable": "Index"},
        title="SPY vs EQX Equal Index",
    )
    comp_fig.update_layout(template="simple_white")
    st.plotly_chart(comp_fig, use_container_width=True)

    st.subheader("Normalized Performance (Base = 100)")
    norm_fig = px.line(
        df,
        x="date",
        y=["eqx_base_100", "spy_base_100"],
        labels={"value": "Normalized Value", "variable": "Index"},
        title="EQX vs SPY (Base 100 Performance)",
    )
    norm_fig.update_layout(template="plotly_dark")
    st.plotly_chart(norm_fig, use_container_width=True)

    st.metric("📈 EQX Total Return (%)", f"{eqx_return:.2f}%")
    st.metric("📊 SPY Total Return (%)", f"{spy_return:.2f}%")

    if "cumulative_return" in df.columns:
        st.subheader("Cumulative Return Over Time")
        cum_fig = px.line(
            df,
            x="date",
            y="cumulative_return",
            title="EQX Equal Index Cumulative Return",
        )
        cum_fig.update_layout(template="plotly_dark")
        st.plotly_chart(cum_fig, use_container_width=True)

    if {"daily_return", "spy_return"}.issubset(df.columns):
        st.subheader("Daily Returns Comparison")
        return_fig = px.line(
            df,
            x="date",
            y=["daily_return", "spy_return"],
            title="Daily Returns: EQX vs SPY",
            labels={"value": "Daily Return", "variable": "Index"},
        )
        return_fig.update_layout(template="plotly_dark")
        st.plotly_chart(return_fig, use_container_width=True)

        st.subheader("Histogram of EQX Daily Returns")
        hist_fig = px.histogram(
            df, x="daily_return", nbins=50, title="EQX Daily Return Distribution"
        )
        hist_fig.update_layout(template="simple_white")
        st.plotly_chart(hist_fig, use_container_width=True)

    if "rolling_volatility" in df.columns:
        st.subheader("7-Day Rolling Volatility")
        vol_fig = px.line(df, x="date", y="rolling_volatility", title="Volatility")
        vol_fig.update_layout(template="plotly_dark")
        st.plotly_chart(vol_fig, use_container_width=True)

    if "drawdown" in df.columns:
        st.subheader("Drawdown Curve")
        draw_fig = px.area(df, x="date", y="drawdown", title="Drawdown Over Time")
        draw_fig.update_traces(line_color="#FFA07A")
        draw_fig.update_layout(template="plotly_dark")
        st.plotly_chart(draw_fig, use_container_width=True)

    if "daily_return" in df.columns:
        best = df.loc[df["daily_return"].idxmax()]
        worst = df.loc[df["daily_return"].idxmin()]
        st.markdown(
            f"✅ **Best Day**: {best['date'].date()} → {best['daily_return']:.2%}"
        )
        st.markdown(
            f"🚨 **Worst Day**: {worst['date'].date()} → {worst['daily_return']:.2%}"
        )

# ----------------------------
# Tab 2 - Explore
# ----------------------------
with tab2:
    st.subheader("Top 100 Tickers by Date")
    date_selected = st.selectbox(
        "Choose a date to view tickers:", df["date"].sort_values(ascending=False)
    )
    row = df[df["date"] == date_selected]
    tickers = (
        [t.strip() for t in row["tickers"].iloc[0].split(",")] if not row.empty else []
    )

    st.markdown(f"Top {len(tickers)} tickers on {date_selected}:")
    st.dataframe(pd.DataFrame({"Ticker": tickers}))

    st.subheader("Rebalancing Changes Between Dates")
    dates = df["date"].unique()
    if len(dates) >= 2:
        date1 = st.selectbox("Select first date", dates, index=0, key="date1")
        date2 = st.selectbox("Select second date", dates, index=1, key="date2")

        def get_set(d):
            val = df[df["date"] == d]["tickers"].iloc[0]
            return set(val.split(",")) if isinstance(val, str) else set(val)

        set1 = get_set(date1)
        set2 = get_set(date2)

        st.write(f"✅ Common Tickers: {len(set1 & set2)}")
        st.write(f"➕ Added on {date2}:")
        st.dataframe(pd.DataFrame({"Added": sorted(set2 - set1)}))
        st.write(f"➖ Removed after {date1}:")
        st.dataframe(pd.DataFrame({"Removed": sorted(set1 - set2)}))
    else:
        st.warning("Not enough dates available for rebalancing comparison.")

# ----------------------------
# Tab 3 - Summary
# ----------------------------
with tab3:
    st.subheader("Summary Metrics")
    if not summary_df.empty:
        for col in summary_df.columns:
            st.metric(
                label=col.replace("_", " ").title(), value=f"{summary_df[col].iloc[0]}"
            )
    else:
        st.warning("Summary metrics not available.")
