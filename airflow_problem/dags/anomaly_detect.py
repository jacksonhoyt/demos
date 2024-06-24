from airflow import DAG
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIFileOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os

def fetch_table_info(table_name, datetime_col, conn):
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name.upper()}'
    AND column_name != '{datetime_col}'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns_info = cursor.fetchall()
    cursor.close()
    return columns_info

def slack_alert_task(): 
    task = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=os.environ['SLACK_CONN'],
        message="Anomalies Found: {{ task_instance.xcom_pull(task_ids='detect_anomalies') }}",
        channel='#airflow-alerts',
    )
    task.execute()


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
    schedule_interval=None,
) as dag:
    

    @task
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

            columns_info = fetch_table_info(table_name, datetime_col, conn)
            dest = f"{table_name.upper()}_ANOMALIES_{datetime.today().strftime('%Y_%m_%d_%H%M')}"

            for column, data_type in columns_info:
                print(f'checking {column} of type {data_type}...')
                if data_type in ['NUMBER', 'FLOAT', 'INTEGER']:
                    query = f"""
                        SELECT '{column}' AS column_name, {column} AS value
                        FROM {table_name}
                        JOIN (SELECT AVG(LAT) POP_AVG, STDDEV(LAT) POP_STDDEV, 3 ZSCORE FROM {table_name})
                        WHERE ABS(({column} - POP_AVG) / POP_STDDEV) > ZSCORE;
                    """
                elif data_type in ['VARCHAR', 'STRING']:
                    query = f"""
                        WITH freq AS (
                            SELECT {column}, COUNT(*) AS cnt
                            FROM {table_name}
                            GROUP BY {column}
                        )
                        SELECT {datetime_col}, '{column}' AS column_name, {column} AS value
                        FROM {table_name}
                        WHERE {column} IN (
                            SELECT {column}
                            FROM freq
                            WHERE cnt < (SELECT AVG(cnt) * 0.05 FROM freq)
                        )
                        AND {datetime_col} >= CURRENT_DATE - INTERVAL '1 DAY'
                    """
                elif data_type in ['DATE', 'TIMESTAMP']:
                    query = f"""
                        SELECT {datetime_col}, '{column}' AS column_name, {column} AS value
                        FROM {table_name}
                        WHERE ({column} < '2000-01-01' OR {column} > CURRENT_DATE)
                        AND {datetime_col} >= CURRENT_DATE - INTERVAL '1 DAY'
                    """
                else:
                    continue
                
                cursor.execute(query)
                report_df = cursor.fetch_pandas_all()
                if not report_df.empty:
                    write_pandas(conn, report_df, dest, auto_create_table=True)

            cursor.close()
            # Send notifications if anomalies found
            if not report_df.empty:
                # slack_alert_task(dest)
                return dest


    anomaly_task = detect_anomalies(table_name='UBER_RAW_DATA_JUN14',datetime_col='DATETIME')

    slack_alert_task = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=os.environ['SLACK_CONN'],
        message="Anomalies Found: {{ task_instance.xcom_pull(task_ids='detect_anomalies') }}",
        channel='#airflow-alerts',
    )

    anomaly_task >> slack_alert_task