import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import numpy as np

DUCKDB_FILE = "eqx_index.db"

# ----------------------------
# Load Index Data
# ----------------------------
def load_index_data():
    conn = duckdb.connect(DUCKDB_FILE)
    try:
        df = conn.execute("SELECT * FROM index_metrics ORDER BY date").fetchdf()
        return df
    except Exception as e:
        st.error(f"Failed to load index data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# ----------------------------
# Load Summary Metrics
# ----------------------------
def load_summary_metrics():
    conn = duckdb.connect(DUCKDB_FILE)
    try:
        df = conn.execute("SELECT * FROM summary_metrics").fetchdf()
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()

# ----------------------------
# List Tables for Debug
# ----------------------------
def list_tables():
    conn = duckdb.connect(DUCKDB_FILE)
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        return [t[0] for t in tables]
    finally:
        conn.close()

# ----------------------------
# Streamlit Layout
# ----------------------------
st.set_page_config(page_title="EQX Index Dashboard", layout="wide")
st.title("EQX Index Dashboard")
st.markdown("Tracking your equal-weighted top 100 US stocks index")

# Debug: show all available tables
with st.expander("Available Tables in DuckDB"):
    st.write(list_tables())

# Load the data
df = load_index_data()
summary_df = load_summary_metrics()

if df.empty:
    st.warning("No index data found. Please run your index builder pipeline first.")
else:
    tab1, tab2, tab3 = st.tabs(["Performance", "Explore", "Summary"])

    with tab1:
        st.subheader("EQX Index Value Over Time")
        fig = px.line(df, x="date", y="index_value", title="EQX Index Performance", markers=True)
        fig.update_traces(line=dict(color="#FF69B4", width=2))
        fig.update_layout(template="simple_white")
        st.plotly_chart(fig, use_container_width=True)

        if {"spy_value", "index_value"}.issubset(df.columns):
            st.subheader("SPY vs EQX Performance")
            comp_fig = px.line(df, x="date", y=["index_value", "spy_value"],
                               labels={"value": "Value", "variable": "Index"},
                               title="SPY vs EQX")
            st.plotly_chart(comp_fig, use_container_width=True)

        st.subheader("Daily Return vs SPY Return")
        if "daily_return" in df.columns and "spy_return" in df.columns:
            return_fig = px.line(df, x="date", y=["daily_return", "spy_return"],
                                 title="Daily Returns Comparison",
                                 labels={"value": "Return", "variable": "Series"})
            st.plotly_chart(return_fig, use_container_width=True)

        st.subheader("Rolling Volatility & Drawdown")
        if "rolling_volatility" in df.columns:
            vol_fig = px.line(df, x="date", y="rolling_volatility", title="Rolling Volatility (7d)")
            st.plotly_chart(vol_fig, use_container_width=True)

        if "drawdown" in df.columns:
            draw_fig = px.area(df, x="date", y="drawdown", title="Drawdown Over Time")
            st.plotly_chart(draw_fig, use_container_width=True)

    with tab2:
        st.subheader("Explore by Date")
        date_selected = st.selectbox("Choose a date to view top 100 tickers:", df["date"].sort_values(ascending=False))
        row = df[df["date"] == date_selected]
        if not row.empty:
            tickers = row["tickers"].iloc[0]
            tickers = list(tickers) if isinstance(tickers, (list, tuple, pd.Series, np.ndarray)) else tickers.split(",")
            weights = [1 / len(tickers)] * len(tickers)

            st.markdown(f"Top {len(tickers)} tickers on {date_selected}:")
            st.dataframe(pd.DataFrame({"Ticker": tickers, "Weight": weights}))

            pie_df = pd.DataFrame({"Ticker": tickers, "Weight": weights})
            pie_fig = px.pie(pie_df, names="Ticker", values="Weight", title="Equal Weights Distribution")
            st.plotly_chart(pie_fig, use_container_width=True)

        st.subheader("Rebalancing Comparison")
        dates = df["date"].unique()
        date1 = st.selectbox("Select first date", dates, index=0, key="date1")
        date2 = st.selectbox("Select second date", dates, index=1, key="date2")

        row1 = df[df["date"] == date1]
        row2 = df[df["date"] == date2]

        if not row1.empty and not row2.empty:
            tickers1 = row1["tickers"].iloc[0]
            tickers2 = row2["tickers"].iloc[0]
            tickers1 = list(tickers1) if isinstance(tickers1, (np.ndarray, list, tuple)) else tickers1.split(",")
            tickers2 = list(tickers2) if isinstance(tickers2, (np.ndarray, list, tuple)) else tickers2.split(",")


            common = list(set(tickers1).intersection(set(tickers2)))
            rebalance_df = pd.DataFrame({
                "Ticker": common,
                date1: [1 / len(tickers1)] * len(common),
                date2: [1 / len(tickers2)] * len(common)
            })

            bar_fig = px.bar(
                rebalance_df.melt(id_vars="Ticker", var_name="Date", value_name="Weight"),
                x="Ticker", y="Weight", color="Date", barmode="group",
                title="Rebalancing Visualization")
            st.plotly_chart(bar_fig, use_container_width=True)

    with tab3:
        st.subheader("Summary Metrics")
        if not summary_df.empty:
            for col in summary_df.columns:
                st.write(f"**{col.replace('_', ' ').title()}**: {summary_df[col].iloc[0]}")
        else:
            st.warning("Summary metrics not available.")
