import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.decorators import task
from astro import sql as aql
import snowflake.connector

@aql.run_raw_sql
def aql_snowflake_connection_test():
    return 'SELECT CURRENT_DATE()'

def snowflake_connetor():
    with snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PW'],
        account=f"{os.environ['SNOWFLAKE_ACCT']}.{os.environ['SNOWFLAKE_REGION']}.aws",
        warehouse=os.environ['SNOWFLAKE_WH'],
        database=os.environ['SNOWFLAKE_DB'],
        schema=os.environ['SNOWFLAKE_SCHEMA']
    ) as con:
        cursor = con.cursor()
        return cursor.execute('SELECT CURRENT_DATE()').fetchall()

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
   
    snowflake_connetor_task = PythonOperator(
        task_id='snowflake_connetor',
        python_callable=snowflake_connetor,
        op_kwargs={
            'table_name': 'YOUR_TABLE_NAME',
            'datetime_col': 'YOUR_DATETIME_COLUMN'
        },
    )

    # https://www.astronomer.io/docs/learn/astro-python-sdk-etl#before-and-after-the-astro-python-sdk
    aql_snowflake_connection_test = aql_snowflake_connection_test(conn_id=os.environ['SNOWFLAKE_CONN'])

    test_snowflake_connection = SnowflakeOperator(
        task_id='test_snowflake_connection',
        sql='SELECT CURRENT_DATE()',
        snowflake_conn_id=os.environ['SNOWFLAKE_CONN']
    )    

    test_snowflake_connection >> aql_snowflake_connection_test

