import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import pytz
from datetime import datetime, timezone, timedelta
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(
    page_title="OpsRev Assessments Dashboard", 
    page_icon="https://media.licdn.com/dms/image/v2/D560BAQFSTXhdraFD5Q/company-logo_200_200/company-logo_200_200/0/1724431599059/groundgame_health_logo?e=2147483647&v=beta&t=m6wbKFRl8Ecxb7ECLTMRp0QLOMTJ-sOjUBBOGWtlNco", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Simple CSS to center the title and move it up
st.markdown("""
    <style>
    h1 {
        text-align: center;
        margin-top: -2.5rem;
        padding-top: 0;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection function with automatic refresh
@st.cache_data(ttl=600)  # Cache for 10 minutes (600 seconds) - fresh data every 10 minutes
def get_data_from_database():
    """Fetch data directly from database with caching"""
    try:
        # Get credentials from Streamlit secrets or environment variables
        if hasattr(st, 'secrets') and 'database' in st.secrets:
            # Streamlit Cloud secrets (TOML format)
            server = st.secrets.database.SERVER
            database = st.secrets.database.DATABASE
            username = st.secrets.database.DB_USERNAME
            password = st.secrets.database.DB_PASSWORD
        else:
            # Environment variables (for local development)
            server = os.getenv('SERVER')
            database = os.getenv('DATABASE')
            username = os.getenv('DB_USERNAME')
            password = os.getenv('DB_PASSWORD')
        
        if not all([server, database, username, password]):
            raise Exception("Missing database credentials. Check .env file or Streamlit secrets.")
        
        # Try different connection methods for cloud compatibility
        connection_methods = [
            # Method 1: pymssql (more cloud-friendly)
            lambda: f"mssql+pymssql://{username}:{password}@{server}/{database}",
            # Method 2: pyodbc with different drivers
            lambda: f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
            lambda: f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
            lambda: f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=FreeTDS&Encrypt=yes&TrustServerCertificate=yes"
        ]
        
        engine = None
        last_error = None
        
        for method in connection_methods:
            try:
                conn_str = method()
                test_engine = create_engine(conn_str)
                # Test the connection
                with test_engine.connect() as test_conn:
                    test_conn.execute(text("SELECT 1"))
                engine = test_engine
                break
            except Exception as e:
                last_error = str(e)
                continue
        
        if not engine:
            # Show debug info in development
            debug_info = f"Could not establish database connection. Last error: {last_error}"
            if not hasattr(st, 'secrets'):  # Only show in local development
                debug_info += f"\nTried connecting to: {server}/{database} with user: {username}"
            raise Exception(debug_info)
# Engine is already created above in the connection method testing
        
        # Query parameters
        outcome_id = 1027
        user_ids = [
            1220, 12431, 3, 1336, 1137, 12432, 12271, 21, 12366, 32,
            1662, 12436, 12437, 12433, 1222, 1404, 12321, 1770, 12476, 12167,
            1992, 19, 12079, 12349, 12082, 12257, 6, 1956, 1785, 4,
            1494, 12231, 1205, 1214, 12478, 12480, 12481
        ]
        
        # September 1st, 2025 at 4 AM EST, converted to UTC for database query
        eastern = pytz.timezone('US/Eastern')
        start_time = eastern.localize(datetime(2025, 9, 1, 4, 0, 0)).astimezone(timezone.utc)
        
        # Build SQL query to get raw individual activity records (no grouping)
        placeholders = ', '.join([f":id{i}" for i in range(len(user_ids))])
        sql_query = f"""
        SELECT 
                USER_ID,
                ACTIVITY_DATE
        FROM FACT_ACTIVITY
        WHERE OUTCOME_ID = :outcome_id
            AND USER_ID IN ({placeholders})
            AND ACTIVITY_DATE >= :start_time
        ORDER BY USER_ID, ACTIVITY_DATE DESC
        """
        
        # Parameters
        params = {"outcome_id": outcome_id, "start_time": start_time}
        params.update({f"id{i}": uid for i, uid in enumerate(user_ids)})
        
        # Execute query
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn, params=params)
            
            # Get max activity date from 2025 for "Data as of" metric
            max_date_2025_query = "SELECT MAX(ACTIVITY_DATE) as max_activity_date FROM FACT_ACTIVITY WHERE YEAR(ACTIVITY_DATE) = 2025"
            max_date_result = pd.read_sql(text(max_date_2025_query), conn)
            max_activity_date_2025 = max_date_result['max_activity_date'].iloc[0]
        
        # Convert ACTIVITY_DATE to datetime and then to Eastern Time
        try:
            df['ACTIVITY_DATE'] = pd.to_datetime(df['ACTIVITY_DATE'])
            eastern = pytz.timezone('US/Eastern')
            df['ACTIVITY_DATE_EST'] = df['ACTIVITY_DATE'].dt.tz_localize('UTC').dt.tz_convert(eastern)
            df['ACTIVITY_DATE_EST'] = df['ACTIVITY_DATE_EST'].dt.tz_localize(None)
            df['ACTIVITY_DATE_ONLY'] = df['ACTIVITY_DATE_EST'].dt.date
        except Exception as dt_error:
            st.warning(f"Date conversion warning: {dt_error}")
            # Fallback: use the original date column
            df['ACTIVITY_DATE_EST'] = df['ACTIVITY_DATE']
            df['ACTIVITY_DATE_ONLY'] = pd.to_datetime(df['ACTIVITY_DATE']).dt.date
        
        # Now group by USER_ID using pandas to get assessments completed and last activity
        grouped_df = df.groupby('USER_ID').agg({
            'ACTIVITY_DATE': 'count',  # Count of assessments
            'ACTIVITY_DATE_EST': 'max'  # Latest activity datetime
        }).reset_index()
        
        # Rename columns for clarity
        grouped_df.columns = ['USER_ID', 'ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME']
        
        # Join with opsrev.csv for names
        try:
            opsrev_df = pd.read_csv('opsrev.csv')
            merged_df = grouped_df.merge(opsrev_df, left_on='USER_ID', right_on='User_Id', how='left')
            result = merged_df[['FULL_NAME', 'LOGIN_ID', 'ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME']].copy()
            result.columns = [col.upper() for col in result.columns]
            return result, datetime.now(), max_activity_date_2025, df  # Return raw data too
        except FileNotFoundError:
            # If opsrev.csv not found, return data without names
            grouped_df.columns = [col.upper() for col in grouped_df.columns]
            return grouped_df, datetime.now(), max_activity_date_2025, df  # Return raw data too
            
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None, None, None, None

# Title
st.title("OpsRev Assessments Dashboard")

# Sidebar with date filter and refresh controls
with st.sidebar:
    st.header("Filters")
    
    # Date filter
    st.subheader("Date Filter")
    
    # Option to filter by single date or date range
    filter_type = st.radio(
        "Filter type:",
        ["Single Date", "Date Range"],
        index=0,  # Default to "Single Date"
        help="Choose to filter by a single date or date range"
    )
    
    if filter_type == "Single Date":
        selected_date = st.date_input(
            "Select date",
            value=pd.to_datetime("today").date(),
            help="Filter data for this specific date"
        )
        date_range = (selected_date, selected_date)  # Convert to range format
    else:
        date_range = st.date_input(
            "Select date range",
            value=(pd.to_datetime("2025-09-01").date(), pd.to_datetime("today").date()),
            help="Filter data by activity date range"
        )
    
    st.markdown("---")
    st.header("Data Controls")

    
    # Use session state to manage the refresh flow
    if 'refresh_clicked' not in st.session_state:
        st.session_state.refresh_clicked = False
    
    # Manual refresh button
    refresh = st.button("🔄 Refresh Data Now")
    
    if refresh:
        st.session_state.refresh_clicked = True
    
    if st.session_state.refresh_clicked:
        password = st.text_input("Enter password to refresh", type="password", key="refresh_password")
        
        if st.button("Submit Password", key="submit_password"):
            # Get refresh password from secrets or environment
            refresh_password = (st.secrets.auth.REFRESH_PASSWORD 
                              if hasattr(st, 'secrets') and 'auth' in st.secrets 
                              else os.getenv('REFRESH_PASSWORD'))
            
            if password == refresh_password:
                st.cache_data.clear()
                st.success("✅ Data refreshed!")
                st.balloons()
                st.session_state.refresh_clicked = False
                st.rerun()
            else:
                st.error("❌ Incorrect password. Data not refreshed.")
        
        if st.button("Cancel", key="cancel_refresh"):
            st.session_state.refresh_clicked = False
            st.rerun()

# Get data
df, last_fetched, max_activity_date_2025, raw_df = get_data_from_database()

if df is not None and raw_df is not None:
    
    # Apply filters to raw data first, then re-group
    filtered_raw = raw_df.copy()
    
    # Filter by date range
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_raw = filtered_raw[
            (filtered_raw['ACTIVITY_DATE_ONLY'] >= start_date) & 
            (filtered_raw['ACTIVITY_DATE_ONLY'] <= end_date)
        ]
    
    # Re-group the filtered raw data
    if len(filtered_raw) > 0:
        filtered_grouped = filtered_raw.groupby('USER_ID').agg({
            'ACTIVITY_DATE': 'count',  # Count of assessments
            'ACTIVITY_DATE_EST': 'max'  # Latest activity datetime
        }).reset_index()
        
        # Rename columns for clarity
        filtered_grouped.columns = ['USER_ID', 'ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME']
        
        # Join with opsrev.csv for names (same as in main function)
        try:
            opsrev_df = pd.read_csv('opsrev.csv')
            merged_filtered = filtered_grouped.merge(opsrev_df, left_on='USER_ID', right_on='User_Id', how='left')
            filtered_df = merged_filtered[['FULL_NAME', 'LOGIN_ID', 'ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME']].copy()
            filtered_df.columns = [col.upper() for col in filtered_df.columns]
        except FileNotFoundError:
            # If opsrev.csv not found, return data without names
            filtered_df = filtered_grouped[['ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME']].copy()
            filtered_df.columns = [col.upper() for col in filtered_df.columns]
    else:
        # No data after filtering
        filtered_df = pd.DataFrame(columns=['FULL_NAME', 'LOGIN_ID', 'ASSESSMENTS_COMPLETED', 'LAST_ASSESSMENT_TIME'])

    # Show metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Users", len(filtered_df))
    with col2:
        st.metric("Total Assessments", int(filtered_df['ASSESSMENTS_COMPLETED'].sum()) if len(filtered_df) > 0 else 0)
    with col3:
        # Get the maximum date from 2025 data
        if pd.isna(max_activity_date_2025):
            formatted_datetime = "No 2025 data available"
        else:
            # Convert UTC to Eastern Time for display
            eastern = pytz.timezone('US/Eastern')
            max_date_utc = pd.to_datetime(max_activity_date_2025).tz_localize('UTC')
            max_date_est = max_date_utc.tz_convert(eastern)
            formatted_datetime = max_date_est.strftime('%Y-%m-%d %H:%M')
        st.metric("Data as of (EST)", formatted_datetime)
    
    # Show table with index starting from 1, sorted by assessments completed (descending)
    df_display = filtered_df.copy()
    df_display = df_display.sort_values('ASSESSMENTS_COMPLETED', ascending=False)
    df_display.index = range(1, len(df_display) + 1)
    
    # Configure column widths
    st.dataframe(
        df_display, 
        use_container_width=True,
        column_config={
            "ASSESSMENTS_COMPLETED": st.column_config.NumberColumn(
                "Assessments",
                width="small",
                format="%d"
            ),
            "FULL_NAME": st.column_config.TextColumn(
                "Full Name",
                width="medium"
            ),
            "LOGIN_ID": st.column_config.TextColumn(
                "Login ID", 
                width="medium"
            ),
            "LAST_ASSESSMENT_TIME": st.column_config.DatetimeColumn(
                "Last Assessment",
                width="medium"
            )
        }
    )
    
    st.markdown("---")
    st.caption("Made with Streamlit · OpsRev Dashboard · 2025")
else:
    st.error("Unable to load data. Please check database connection.")
