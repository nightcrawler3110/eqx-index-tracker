import pandas as pd
import streamlit as st
from pathlib import Path

from src.config import Config  # Centralized config

# --- Page Setup ---
st.set_page_config(page_title="Validation Report", layout="wide")
st.title("📊 Data Validation Report")
st.markdown("Review validation issues and inspect problematic records across tables and columns.")

# --- Load Summary Report ---
validation_report_path = Path(Config.VALIDATION_REPORT)

if not validation_report_path.exists():
    st.error("Validation report not found. Please run `data_validations.py` first.")
    st.stop()

try:
    df_summary = pd.read_csv(validation_report_path)
    if df_summary.empty:
        st.info("No validation issues found. All data passed checks.")
        st.stop()
except Exception as e:
    st.error(f"Failed to read validation report: {e}")
    st.stop()

# --- Summary Table ---
st.subheader("📌 Validation Summary")
st.dataframe(df_summary, use_container_width=True)

# --- Detailed Issues Viewer ---
st.subheader("🔍 Explore Issue Details")

grouped = df_summary.groupby("table")

for table, group in grouped:
    with st.expander(f"🗂️ Table: `{table}` — {len(group)} issues", expanded=False):
        for _, row in group.iterrows():
            issue_type = row["issue"]
            column = row["column"]
            count = row["count"]
            details_file = row["details_file"]
            file_path = Path(details_file) if pd.notna(details_file) else None

            label = f"{issue_type} in `{column}` ({count} rows)"

            if file_path and file_path.exists():
                with st.expander(label, expanded=False):
                    try:
                        df_issue = pd.read_csv(file_path)
                        st.dataframe(df_issue, use_container_width=True, height=300)
                    except Exception as e:
                        st.warning(f"⚠️ Failed to read issue file: {file_path.name} — {e}")
            else:
                st.markdown(f"- ❌ {label} _(Details file missing or not generated)_")
