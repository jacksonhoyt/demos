import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.decorators import task
from astro import sql as aql

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
    'retry_delay': timedelta(seconds=20),
}
# Define the basic parameters of the DAG, like schedule and start_date
with DAG(
    dag_id='snowflake_connect_test',
    start_date=datetime(2024, 6, 22),
    schedule=None,
    catchup=False,
    description='DAG to Snowflake connection',
    default_args=default_args,
    tags=["example"],
):
   
    # https://www.astronomer.io/docs/learn/astro-python-sdk-etl#before-and-after-the-astro-python-sdk
    aql_snowflake_connection_test = aql_snowflake_connection_test(conn_id=os.environ['SNOWFLAKE_CONN'])

    test_snowflake_connection = SnowflakeOperator(
        task_id='test_snowflake_connection',
        sql='SELECT CURRENT_DATE()',
        snowflake_conn_id=os.environ['SNOWFLAKE_CONN']
    )    

    test_snowflake_connection >> aql_snowflake_connection_test

