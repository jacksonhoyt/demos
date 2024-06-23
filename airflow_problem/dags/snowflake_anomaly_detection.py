import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
from astro import sql as aql
import pandas as pd



@aql.run_raw_sql
def aql_snowflake_connection_test():
    return 'SELECT CURRENT_DATE()'


# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'snowflake_anomaly_detection',
    default_args=default_args,
    description='DAG to detect anomalies in Snowflake table using SQL and notify via Slack',
    schedule_interval=None,
    start_date=datetime(2024, 6, 22),
):
    aql_snowflake_connection_test = aql_snowflake_connection_test(conn_id=os.environ['SNOWFLAKE_CONN'])


    @task
    def build_slack_message():
        return f"""
        This is a test message from Airflow.
        Sent at {datetime(2024, 6, 22)}
        """

    build_slack_message_task = build_slack_message()

    slack_alert_task = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=os.environ['SLACK_CONN'],
        message="{{ task_instance.xcom_pull(task_ids='build_slack_message') }}",
        channel='#airflow-alerts',
    )

    build_slack_message_task >> slack_alert_task

    # @task
    # def detect_anomalies_sql(table_name: str, datetime_column: str):
    #     # Helper function to get numeric columns from the Snowflake table
    #     def get_numeric_columns():
    #         query = f"""
    #             SELECT COLUMN_NAME
    #             FROM INFORMATION_SCHEMA.COLUMNS
    #             WHERE TABLE_NAME = '{table_name}'
    #             AND DATA_TYPE IN ('NUMBER', 'FLOAT', 'DOUBLE', 'INT')
    #         """
    #         numeric_columns_df = database.run_sql(query)
    #         return numeric_columns_df['COLUMN_NAME'].tolist()

    #     numeric_columns = get_numeric_columns()

    #     # Query to calculate mean and standard deviation for each numeric column
    #     stats_query = f"""
    #         SELECT
    #             AVG(column_value) AS mean,
    #             STDDEV(column_value) AS stddev,
    #             column_name
    #         FROM (
    #             SELECT
    #                 {datetime_column},
    #                 value AS column_value,
    #                 column_name
    #             FROM {table_name}
    #             UNPIVOT (value FOR column_name IN ({', '.join(numeric_columns)}))
    #         )
    #         WHERE {datetime_column} >= DATEADD(day, -30, CURRENT_DATE)
    #         GROUP BY column_name
    #     """
        
    #     stats_df = database.run_sql(stats_query)

    #     # Query to detect anomalies using calculated mean and stddev
    #     anomaly_query = "SELECT * FROM ("
    #     for index, row in stats_df.iterrows():
    #         mean = row['mean']
    #         stddev = row['stddev']
    #         column_name = row['column_name']
    #         threshold = 3

    #         if index > 0:
    #             anomaly_query += " UNION ALL "
            
    #         anomaly_query += f"""
    #             SELECT *
    #             FROM {table_name}
    #             WHERE ABS({column_name} - {mean}) > {threshold} * {stddev}
    #             AND {datetime_column} >= DATEADD(day, -30, CURRENT_DATE)
    #         """
    #     anomaly_query += ")"

    #     anomalies_df = database.run_sql(anomaly_query)
    #     anomalies_file_path = '/tmp/anomalies.csv'
    #     anomalies_df.to_csv(anomalies_file_path, index=False)
    #     return anomalies_file_path

    # @task
    # def prepare_alert_content(anomalies_file_path: str):
    #     with open(anomalies_file_path, 'r') as f:
    #         anomalies = f.read()
    #     return anomalies

                                                                                # @task
                                                                                # def build_slack_message(anomalies: str):
    #     if anomalies:
    #         return f"Anomalies detected:\n{anomalies}"
    #     return "No anomalies detected."

    # # Detect anomalies in the data using SQL
    # anomaly_task = detect_anomalies_sql(table_name='your_table_name',datetime_column='your_datetime_column')

    # # Prepare alert content if anomalies are found
    # prepare_alert_task = prepare_alert_content(anomalies_file_path=anomaly_task.output)

                                                                                # # Build Slack message content
                                                                                # build_slack_message_task = build_slack_message(anomalies=prepare_alert_task.output)

                                                                                # Send Slack notification if anomalies are found
                                                                                # slack_alert_task = SlackWebhookOperator(
                                                                                #     task_id='send_slack_alert',
                                                                                #     http_conn_id='slack_conn_id',  # Slack connection ID configured in Airflow
                                                                                #     message="{{ task_instance.xcom_pull(task_ids='build_slack_message') }}",
                                                                                #     channel='#airflow-alerts',  # Replace with your Slack channel
                                                                                # )

    # anomaly_task >> prepare_alert_task >> build_slack_message_task >> slack_alert_task
