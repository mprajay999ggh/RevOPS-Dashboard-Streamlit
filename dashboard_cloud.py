import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import pytz
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(
    page_title="RevOps Assessments Dashboard", 
    page_icon="https://media.licdn.com/dms/image/v2/D560BAQFSTXhdraFD5Q/company-logo_200_200/company-logo_200_200/0/1724431599059/groundgame_health_logo?e=2147483647&v=beta&t=m6wbKFRl8Ecxb7ECLTMRp0QLOMTJ-sOjUBBOGWtlNco", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

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
    h1 {
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection function
@st.cache_data(ttl=1800)  # Cache for 30 minutes (1800 seconds)
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
        
        # Today at 4 AM UTC
        start_time = datetime.now(timezone.utc).replace(hour=4, minute=0, second=0, microsecond=0)
        
        # Build SQL query
        placeholders = ', '.join([f":id{i}" for i in range(len(user_ids))])
        sql_query = f"""
        SELECT 
                USER_ID,
                COUNT(*) AS ASSESSMENTS_COMPLETED,
                MAX(ACTIVITY_DATE) AS last_activity_date
        FROM FACT_ACTIVITY
        WHERE OUTCOME_ID = :outcome_id
            AND USER_ID IN ({placeholders})
            AND ACTIVITY_DATE >= :start_time
        GROUP BY USER_ID
        ORDER BY ASSESSMENTS_COMPLETED DESC
        """
        
        # Parameters
        params = {"outcome_id": outcome_id, "start_time": start_time}
        params.update({f"id{i}": uid for i, uid in enumerate(user_ids)})
        
        # Execute query
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn, params=params)
        
        # Convert UTC to Eastern Time
        eastern = pytz.timezone('US/Eastern')
        df['last_activity_date_est'] = df['last_activity_date'].dt.tz_localize('UTC').dt.tz_convert(eastern)
        df['last_activity_date_est'] = df['last_activity_date_est'].dt.tz_localize(None)
        
        # Join with revops.csv for names
        try:
            revops_df = pd.read_csv('revops.csv')
            merged_df = df.merge(revops_df, left_on='USER_ID', right_on='User_Id', how='left')
            result = merged_df[['USER_ID', 'FULL_NAME', 'LOGIN_ID', 'ASSESSMENTS_COMPLETED', 'last_activity_date_est']].copy()
            result.columns = [col.upper() for col in result.columns]
            return result, datetime.now()
        except FileNotFoundError:
            # If revops.csv not found, return data without names
            df.columns = [col.upper() for col in df.columns]
            return df, datetime.now()
            
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None, None

# Title and description with logo
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    #st.image("https://media.licdn.com/dms/image/v2/D560BAQFSTXhdraFD5Q/company-logo_200_200/company-logo_200_200/0/1724431599059/groundgame_health_logo?e=2147483647&v=beta&t=m6wbKFRl8Ecxb7ECLTMRp0QLOMTJ-sOjUBBOGWtlNco", width=150)
    st.title("RevOps Assessments Dashboard")

# Sidebar with refresh controls
with st.sidebar:
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
df, last_fetched = get_data_from_database()

if df is not None:

    # Show metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Users", len(df))
    with col2:
        st.metric("Total Assessments", int(df['ASSESSMENTS_COMPLETED'].sum()))
    with col3:
        # Get the maximum date from the data
        max_date = df['LAST_ACTIVITY_DATE_EST'].max()
        formatted_datetime = pd.to_datetime(max_date).strftime('%Y-%m-%d %H:%M')
        st.metric("Data as of", formatted_datetime)
    
    # Show table
    st.dataframe(df, width='stretch')
    
    st.markdown("---")
    st.caption("Made with Streamlit · RevOps Dashboard · 2025")
else:
    st.error("Unable to load data. Please check database connection.")
