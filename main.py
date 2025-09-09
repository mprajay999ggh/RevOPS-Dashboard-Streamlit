from datetime import datetime
from datetime import timezone
import pandas as pd
from sqlalchemy import create_engine, text
import pytz  # For timezone conversion
from dotenv import load_dotenv
import os

## ------------------------
# Load sensitive info from .env
## ------------------------
load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

conn_str = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
)
engine = create_engine(conn_str)

# ------------------------
# Query parameters
# ------------------------
outcome_id = 1027
user_ids = [
    1220, 12431, 3, 1336, 1137, 12432, 12271, 21, 12366, 32,
    1662, 12436, 12437, 12433, 1222, 1404, 12321, 1770, 12476, 12167,
    1992, 19, 12079, 12349, 12082, 12257, 6, 1956, 1785, 4,
    1494, 12231, 1205, 1214, 12478, 12480, 12481
]

# Today at 4 AM UTC
start_time = datetime.now(timezone.utc).replace(hour=4, minute=0, second=0, microsecond=0)
print(f"⏱️ Start time (UTC): {start_time}")
# ------------------------
# Build SQL query with aggregation
# ------------------------
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

# Combine parameters into dictionary
params = {"outcome_id": outcome_id, "start_time": start_time}
params.update({f"id{i}": uid for i, uid in enumerate(user_ids)})

# ------------------------
# Execute query and load into pandas
# ------------------------
with engine.connect() as conn:
    df = pd.read_sql(text(sql_query), conn, params=params)

# ------------------------
# Convert UTC to Eastern Time (EST/EDT)
# ------------------------
eastern = pytz.timezone('US/Eastern')
df['last_activity_date_est'] = df['last_activity_date'].dt.tz_localize('UTC').dt.tz_convert(eastern)

# Optionally, remove timezone info if you want naive datetime
df['last_activity_date_est'] = df['last_activity_date_est'].dt.tz_localize(None)

print(f"✅ Retrieved {len(df)} users")

# ------------------------
# Join with revops.csv to get name and email
# ------------------------
revops_df = pd.read_csv('revops.csv')

merged_df = df.merge(revops_df, left_on='USER_ID', right_on='User_Id', how='left')
result = merged_df[['USER_ID', 'FULL_NAME', 'LOGIN_ID', 'ASSESSMENTS_COMPLETED', 'last_activity_date_est']].copy()
result.columns = [col.upper() for col in result.columns]

print(result)
result.to_csv('result.csv', index=False)

print("✅ Saved results to result.csv for dashboard.")

# Automate git commit and push for result.csv
import subprocess
try:
    subprocess.run(['git', 'add', 'result.csv'], check=True)
    subprocess.run(['git', 'commit', '-m', 'Update dashboard data'], check=True)
    subprocess.run(['git', 'push'], check=True)
    print("✅ CSV committed and pushed to GitHub.")
except Exception as e:
    print(f"⚠️ Git automation failed: {e}")
