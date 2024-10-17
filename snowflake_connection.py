import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
from alerts import create_weather_alerts_csv

# Load environment variables
load_dotenv()

def create_weather_alerts_snowflake():
    # Create Snowflake objects
    # create_snowflake_objects()
    alerts = create_weather_alerts_csv()

    # Convert all column names to uppercase
    alerts.columns = alerts.columns.str.upper()

    # Connect to Snowflake with the newly created objects
    conn = snowflake.connector.connect(
        account=os.getenv('account'),
        user=os.getenv('user'),
        password=os.getenv('password'),
        warehouse='WEATHER_WAREHOUSE',
        database='WEATHER_DATA',
        schema='WEATHER_SCHEMA'
    )

    try:
        # Create table if it doesn't exist
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS WEATHER_ALERTS (
                ID STRING,
                AREADESC STRING,
                SAME_CODES STRING,
                UGC_CODES STRING,
                AFFECTEDZONES STRING,
                SENT TIMESTAMP_NTZ,
                EFFECTIVE TIMESTAMP_NTZ,
                ONSET TIMESTAMP_NTZ,
                EXPIRES TIMESTAMP_NTZ,
                ENDS TIMESTAMP_NTZ,
                STATUS STRING,
                MESSAGETYPE STRING,
                CATEGORY STRING,
                SEVERITY STRING,
                CERTAINTY STRING,
                URGENCY STRING,
                EVENT STRING,
                HEADLINE STRING,
                COORDINATES STRING
            )
            """)

        # Insert data into Snowflake
        success, nchunks, nrows, _ = write_pandas(conn, alerts, 'WEATHER_ALERTS')
        
        print(f"Data inserted successfully into Snowflake. {nrows} rows inserted.")

    finally:
        conn.close()

if __name__ == "__main__":
    create_weather_alerts_snowflake()