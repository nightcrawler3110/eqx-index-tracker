from pathlib import Path
import streamlit as st
import pandas as pd
import os

# --- Config ---
BASE_DIR = Path(__file__).resolve().parent
VALIDATION_REPORT = BASE_DIR / "reports" / "data_validation_report.csv"
DETAILS_DIR = BASE_DIR / "reports" / "detailed_issues"

# --- Page Setup ---
st.set_page_config(page_title="Validation Report", layout="wide")
st.title("📊 Data Validation Report")
st.markdown("Review issues and explore problematic records across tables.")

# --- Load Summary Report ---
if not VALIDATION_REPORT.exists():
    st.error("Validation report not found. Please run `data_validations.py` first.")
    st.stop()

df_summary = pd.read_csv(VALIDATION_REPORT)

# --- Summary Table ---
st.subheader("📌 Validation Summary")
st.dataframe(df_summary, use_container_width=True)

# --- Detailed Issues ---
st.subheader("🔍 Explore Issue Details")

grouped = df_summary.groupby("table")

for table, group in grouped:
    with st.expander(f"Table: `{table}` — {len(group)} issues", expanded=False):
        for _, row in group.iterrows():
            issue_type = row["issue"]
            column = row["column"]
            count = row["count"]
            file_path = BASE_DIR / row["details_file"] if pd.notna(row["details_file"]) else None

            label = f"{issue_type} in `{column}` ({count} rows)"
            if file_path and file_path.exists():
                with st.expander(label, expanded=False):
                    df_issue = pd.read_csv(file_path)
                    st.dataframe(df_issue, use_container_width=True, height=300)
            else:
                st.markdown(f"- {label} _(Details file missing or not generated)_")
