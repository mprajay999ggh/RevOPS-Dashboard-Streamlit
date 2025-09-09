import streamlit as st
import pandas as pd
import subprocess
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables
load_dotenv()


st.set_page_config(page_title="RevOps Assessments Dashboard", page_icon="📊", layout="wide")

st.markdown("""
    <style>
    .main {
        background-color: #f5f7fa;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        border-radius: 12px;
        background: #fff;
        box-shadow: 0 2px 16px rgba(0,0,0,0.07);
    }
    .stDataFrame th, .stDataFrame td {
        font-size: 1.1em;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📊 RevOps Assessments Dashboard")
st.write("View the number of assessments completed by each user, along with their name and email.")

# --- Refresh CSV from GitHub ---
with st.sidebar:
    st.header("Refresh Data from GitHub")
    refresh = st.button("Refresh CSV from GitHub")
    if refresh:
        password = st.text_input("Enter password to refresh", type="password")
        if password:
            if password == os.getenv('REFRESH_PASSWORD'):
                with st.spinner("Updating data from database and GitHub..."):
                    try:
                        # Run main.py to update result.csv from DB
                        subprocess.run(["python", "main.py"], check=True)
                        # Optionally, pull latest from GitHub (if needed)
                        subprocess.run(["git", "pull"], check=True)
                        st.success("CSV refreshed from DB and GitHub!")
                        st.balloons()
                        # Force rerun to show updated data
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to refresh: {e}")
            else:
                st.error("Incorrect password. CSV not refreshed.")


# Load data (only from CSV, not from DB)
try:
    df = pd.read_csv('result.csv')
    # Get file modification time to show when data was last updated
    import os
    file_mod_time = os.path.getmtime('result.csv')
    last_updated = datetime.fromtimestamp(file_mod_time).strftime("%Y-%m-%d %H:%M:%S")
    st.sidebar.info(f"📅 Data last updated: {last_updated}")
except FileNotFoundError:
    st.error("No data found. Please make sure 'result.csv' exists and is updated in GitHub.")
    st.stop()


# Sidebar filters (below refresh)
st.sidebar.header("Filters")
name_filter = st.sidebar.text_input("Search by Name")
email_filter = st.sidebar.text_input("Search by Email")
min_assessments = st.sidebar.slider("Minimum Assessments Completed", 0, int(df['ASSESSMENTS_COMPLETED'].max()), 0)


# Filter data
filtered_df = df.copy()
if name_filter:
    filtered_df = filtered_df[filtered_df['FULL_NAME'].str.contains(name_filter, case=False, na=False)]
if email_filter:
    filtered_df = filtered_df[filtered_df['LOGIN_ID'].str.contains(email_filter, case=False, na=False)]
filtered_df = filtered_df[filtered_df['ASSESSMENTS_COMPLETED'] >= min_assessments]


# Show metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Users", len(filtered_df))
with col2:
    st.metric("Total Assessments", int(filtered_df['ASSESSMENTS_COMPLETED'].sum()))
with col3:
    st.metric("Data Age", f"{(datetime.now() - datetime.fromtimestamp(file_mod_time)).days} days")

# Show table
st.dataframe(filtered_df, use_container_width=True)

st.markdown("---")
st.caption("Made with Streamlit · RevOps Dashboard · 2025")
