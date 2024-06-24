"""
## Anomaly Detection for Columns in Snowflake Table

DAG accepts a Snowflake table name and that table's primary datetime column, and then checks every other column in the table for anomalies.

If anomalies are found, the anomalous data is written to a Snowflake table and a slack alert is issued to #airflow-alerts.

#### Options for calling DAG
- airflow UI can call this DAG and user can provide table_name and datetime_column
- upstream DAGs can call this DAG using `TriggerDagRunOperator('anomaly_detect', conf = {"table_name": "my_table", "datetime_column": "my_datetime_column"}, ...)`
- airflow CLI can call this DAG using `airflow dags trigger 'anomaly_detect' --conf '{"table_name": "my_table", "datetime_column": "my_datetime_column"}'`
- airflow REST API can call this DAG using something like the following (see https://airflow.apache.org/docs/apache-airflow/2.0.1/stable-rest-api-ref.html for options):
    ```bash
    curl --location --request POST 'localhost:8080/api/v1/dags/unpublished/dagRuns'
    --header 'Content-Type: application/json'
    --header 'Authorization: Basic YWRtaW46YWRtaW4='
    --data-raw '{
        "conf": {
            "table_name": "table_name",
            "datetime_column": "my_datetime_column"
        }
    }'
    ```
"""

from airflow import DAG
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIFileOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

def fetch_table_info(table_name, datetime_col, cursor):
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name.upper()}'
        AND column_name != '{datetime_col}'
    """
    return cursor.execute(query).fetchall()

def df_to_snowflake(conn,query,dest):
    df = conn.cursor().execute(query).fetch_pandas_all()
    if not df.empty:
        write_pandas(conn, df, dest, auto_create_table=True)
    return df

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    'anomaly_detect',
    default_args=default_args,
    description='A simple anomaly detection DAG',
    doc_md=__doc__,
    schedule_interval=None,
    start_date=datetime(2024, 6, 22),
    catchup=False,
    params={"table_name": "UBER_RAW_DATA_JUN14", "datetime_column": "DATETIME"}, # when calling the DAG, provide this config with the desired table_name and datetime_column values
) as dag:
    
    @task(task_id='detect_anomalies')
    def detect_anomalies(table_name, datetime_col, **kwargs):
        with snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PW'],
            account=f"{os.environ['SNOWFLAKE_ACCT']}.{os.environ['SNOWFLAKE_REGION']}.aws",
            warehouse=os.environ['SNOWFLAKE_WH'],
            database=os.environ['SNOWFLAKE_DB'],
            schema=os.environ['SNOWFLAKE_SCHEMA']
        ) as conn:
            cursor = conn.cursor()

            columns_info = fetch_table_info(table_name, datetime_col, cursor) # columns to check from table_name
            print(columns_info)
            dest = f"{table_name.upper()}_ANOMALIES_{datetime.today().strftime('%Y_%m_%d_%H%M')}" # destination table if anything is reported
            reporting = False # mark true if anything is reported

            # Data Type and Null checks
            for column, data_type in columns_info:
                print(f'checking {column} of type {data_type}...')
                if data_type in ['NUMBER', 'FLOAT', 'INTEGER']:
                    # Numeric Checks
                    # Check if column values are >3 standard deviations away from the mean
                    query = f"""
                        SELECT TO_VARCHAR({datetime_col}) date_info, '{column}' AS column_name, TO_VARCHAR({column}) AS value, 'ZSCORE > 3' check_type
                        FROM {table_name}
                        JOIN (SELECT AVG(LAT) POP_AVG, STDDEV(LAT) POP_STDDEV, 3 ZSCORE FROM {table_name})
                        WHERE ABS(({column} - POP_AVG) / POP_STDDEV) > ZSCORE;
                    """
                elif data_type in ['VARCHAR', 'STRING', 'TEXT']:
                    # Categorical Checks
                    # Check if column has infrequent values which occur <5% of the average frequency for all unique values
                    query = f"""
                        WITH freq AS (
                            SELECT {column}, COUNT(*) AS cnt
                            FROM {table_name}
                            GROUP BY 1
                        )
                        SELECT TO_VARCHAR(a.{datetime_col}) date_info, '{column}' AS column_name, a.{column} AS value, 'CATEGORY FREQUENCY < 7%' check_type
                        FROM {table_name} a
                        WHERE a.{column} IN (
                            SELECT {column} FROM freq
                            WHERE cnt < (SELECT AVG(cnt) * 0.05 FROM freq)
                        )
                    """
                elif data_type in ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ']:
                    # Temporal Checks
                    # Check if datetime is beyond expected date range
                    query = f"""
                        SELECT TO_VARCHAR({datetime_col}) date_info, '{column}' AS column_name, TO_VARCHAR({column}) AS value, 'DATE RANGE ISSUE' check_type
                        FROM {table_name}
                        WHERE ({column} < '2000-01-01' OR {column} > CURRENT_DATE)                    
                    """
                else:
                    continue
                
                report_df = df_to_snowflake(conn,query,dest)

                # NULL Checks
                # Check if NULL values are >5% of column by year-month
                null_query = f"""
                    SELECT 
                    TO_CHAR({datetime_col}, 'YYYY-MM') AS year_month
                    , '{column}' AS column_name
                    , (COUNT_IF({column} IS NULL) / COUNT(*)) * 100 AS null_percentage
                    , 'NULL ROWS > 5% OF TOTAL ROWS BY YEAR-MONTH' AS check_type
                    FROM {table_name}
                    GROUP BY 1
                    HAVING null_percentage > 5
                """
                null_report_df = df_to_snowflake(conn,null_query,dest)

                if not null_report_df.empty or not report_df.empty:
                    reporting = True

            # Row Count Checks
            # Check if row count change is >50% to previous year-month
            row_count_query = f"""
                WITH row_counts AS (
                    SELECT TO_CHAR({datetime_col}, 'YYYY-MM') AS year_month, COUNT(*) AS row_count
                    FROM {table_name}
                    GROUP BY TO_CHAR({datetime_col}, 'YYYY-MM')
                ), row_count_change AS (
                    SELECT 
                        year_month, 
                        row_count, 
                        LAG(row_count) OVER (ORDER BY year_month) AS prev_row_count,
                        (row_count - LAG(row_count) OVER (ORDER BY year_month)) / NULLIF(LAG(row_count) OVER (ORDER BY year_month), 0) * 100 AS change_percentage
                    FROM row_counts
                    QUALIFY ABS(change_percentage) > 50
                )
                SELECT year_month AS date_info
                , 'ROW COUNT CHANGE TO PREVIOUS MONTH IS HIGH' AS column_name
                , TO_VARCHAR(ROUND(change_percentage,2) || '%') AS value
                , 'ROW COUNT > 50% CHANGE FROM PREVIOUS YEAR-MONTH' check_type
                FROM row_count_change
            """
            row_count_report_df = df_to_snowflake(conn,row_count_query,dest)
            if not row_count_report_df.empty:
                reporting = True

            cursor.close()
            # Trigger notifications if anomalies found
            if reporting:
                return dest
    
    # check the provided table_name for anomalies in the data using the datetime_column
    anomaly_task = detect_anomalies(table_name='{{ dag_run.conf["table_name"] }}',datetime_col='{{ dag_run.conf["datetime_column"] }}') # works when triggered from airflow CLI `airflow dags trigger 'anomaly_detect' --conf '{"table": "UBER_RAW_DATA_JUN14"}'` 
    # if detect_anomalies returned a value trigger the slack task 
    @task.branch(task_id='branch_task')
    def branch_func(ti=None):
        xcom_value = ti.xcom_pull(task_ids="detect_anomalies")
        return 'send_slack_message' if xcom_value else None
    
    # send slack message stating the table name where anomalies can be reviewed (ie: [original table]_ANOMALIES_[yyyy_mm_dd_HHMM])
    slack_alert_task = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=os.environ['SLACK_CONN'],
        message="Anomalies Found: {{ task_instance.xcom_pull(task_ids='detect_anomalies') }}",
        channel='#airflow-alerts',
    )

    anomaly_task >> branch_func() >> slack_alert_task