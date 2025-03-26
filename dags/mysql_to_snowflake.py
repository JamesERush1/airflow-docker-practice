import os
import pandas as pd
import mysql.connector
import snowflake.connector
from datetime import datetime

def migrate_data(execution_date):
    # Parse execution_date
    run_date = datetime.strptime(execution_date, "%Y-%m-%d").date()

    # MySQL connection
    mysql_conn = mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DB"]
    )

    query = f"""
    SELECT * FROM your_table
    WHERE DATE(created_at) = '{run_date}'
    """

    df = pd.read_sql(query, mysql_conn)
    mysql_conn.close()

    print(f"Fetched {len(df)} rows from MySQL for {run_date}")

    # Snowflake connection
    sf_conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )

    # Upload to Snowflake
    success, nchunks, nrows, _ = sf_conn.write_pandas(
        df,
        table_name="your_snowflake_table",
        quote_identifiers=False
    )

    sf_conn.close()

    print(f"Uploaded {nrows} rows to Snowflake on {run_date}")
