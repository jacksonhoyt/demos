Overview
========

The DAG `anomaly_detect` in `./dags/anomaly_detect.py` is a pipeline built in Apache Airflow designed to do the following high level steps.
1. Receive a Snowflake Table name and a given datetime column
2. Fetch column information on the Snowflake Table
3. Detect anomalies using five different types of checks
    - (Numeric, Categorical, Temporal, Null, Row Count)
4. Report anomalies in a new Snowflake table
5. Send Slack alert if anomalies were recorded

Each part is described further below under Details.

<table>
  <th colspan=2>anomaly_detect representation</th>
  <tr>
    <td>When anomalies are found</td>
    <td><img src="https://github.com/jacksonhoyt/lennar/blob/main/airflow_problem/anomaly_detect_finds_anomaly.png?raw=true" alt="Anomaly Detected DAG Image" width = 640px>
    </td>
   </tr> 
   <tr>
    <td>When no anomalies are found</td>
    <td><img src="https://github.com/jacksonhoyt/lennar/blob/main/airflow_problem/anomaly_detect_finds_no_anomaly.png?raw=true" alt="Anomaly Not Detected DAG Image" width = 640px>
    </td>
  </td>
  </tr>
</table>

##### Table of Contents  
- [Assumptions](#assumptions)  
- [Details](#details)  
    - [DAG Triggers](#dagtriggers)
    - [DAG Configuration](#dagconfig)
    - [Snowflake Connection](#snowflakeconnect)
    - [Fetch Columns](#fetchcolumns)
    - [Detect And Report Anomalies](#detectanomalies)
        - [Numeric Check](#numeric)
        - [Categorical Check](#categorical)
        - [Temporal Check](#temporal)
        - [Evaluate Data Type Checks](#evaldatatypes)
        - [Null Check](#nullcheck)
        - [Row Count Check](#rowcount)
    - [Send Slack Alerts](#slackalert)



<a name="assumptions"/>

Assumptions
========
##### Environment
- An Airflow environment with Python and libraries in `requirements.txt` installed
- DAG is triggered manually (via Airflow UI, Airflow CLI, or REST API call) and provided the table name and datetime column as config parameters
- A Slack app with valid webhook
- Snowflake hosts the table

##### Data
- The table and datetime column are valid for historical data analysis
- Table schema is available in the Snowflake information_schema

##### Credentials
- Snowflake credentials, along with database and schema, are stored in environment variables
- Slack credentials for alerts are predefined in Connections or airflow_settings.yaml

#### Anomalies
- z-score of 3 is sufficient for capturing Numeric anomalies
- 5% of average frequency is sufficient for capturing categorical anomalies
- Future dates or dates prior to year 2000 are sufficient bounds for capturing temporal anomalies
- 5% is sufficient threshold for capturing year-months with more than 5% records being NULL
- 50% is sufficient threshold for capturing year-months with more than 50% shift in row counts to previous month

<a name="details"/>

Details
========
<a name="dagtriggers"/>

#### DAG Triggers
Any of the following are valid ways for this DAG to be triggered. It would be recommended for an upstream process to call this DAG at any stage of ETL to perform anomaly detection, whether that is done for raw input data or transformed data.
- Trigger DAG via Airflow UI and user inputs table_name and datetime_column
- Trigger DAG via upstream DAG using `TriggerDagRunOperator('anomaly_detect', conf = {"table_name": "my_table", "datetime_column": "my_datetime_column"}, ...)`
- Trigger DAG via Airflow CLI using `airflow dags trigger 'anomaly_detect' --conf '{"table_name": "my_table", "datetime_column": "my_datetime_column"}'`
- Trigger DAG via REST API can call this DAG using a POST similar to the following (see https://airflow.apache.org/docs/apache-airflow/2.0.1/stable-rest-api-ref.html for options):
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

<a name="dagconfig"/>

#### DAG Configuration
Note the `schedule_internal=None` as this is never scheduled to run on its own. However, when the DAG is triggered it is `params={"table_name": "MY_TABLE", "datetime_column": "MY_DATETIME_COLUMN"}` that prepares the DAG to receive the **table** and **datetime** inputs.

```python
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
    params={"table_name": "MY_TABLE", "datetime_column": "MY_DATETIME_COLUMN"}, # when calling the DAG, provide this config with the desired table_name and datetime_column values
) as dag:
```

<a name="snowflakeconnect"/>

#### Establish Snowflake connection and cursor object
The Snowflake connection and cursor are established in `detect_anomalies` via `with snowflake.connector.connect(...) as conn:` to ensure the connection closes when the task ends. The Snowflake connection is only used inside `detect_anomalies` and its subprocess shown next.

```python
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
        ...
```

<a name="fetchcolumns"/>

#### Fetch Columns
`detect_anomalies` calls the subprocess `fetch_table_info` to retrieve column metadata on the **table** from Snowflake's `information_schema.columns`. The `data_type` will inform `detect_anomalies` what method of anomaly detection to use.

If anomalies are found, `dest` is where the data will be written in Snowflake and `reporting` will be changed to True to trigger Slack alerts. These will have more context throughout the following sections.

```python
@task(task_id='detect_anomalies')
def detect_anomalies(table_name, datetime_col, **kwargs):
    ...
    columns_info = fetch_table_info(table_name, datetime_col, cursor) # columns to check from table_name
    dest = f"{table_name.upper()}_ANOMALIES_{datetime.today().strftime('%Y_%m_%d_%H%M')}" # destination table if anything is reported
    reporting = False # mark true if anything is reported
    ...
```

```python
def fetch_table_info(table_name, datetime_col, cursor):
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name.upper()}'
        AND column_name != '{datetime_col}'
    """
    return cursor.execute(query).fetchall()
```

<a name="detectanomalies"/>

#### Detect and Report Anomalies
Performs various checks for anomalies across different data types and record anomalous data to Snowflake `dest`. All checks are done via SQL to leverage Snowflake's compute and processing power, as opposed to pulling data into memory and analyzing via one large Dataframe.

Begin by looping through `columns_info` to perform a check on every column found in the **table** based on the **data_type**. Anomaly detection is provided for the following specific data types.
- 'NUMBER', 'FLOAT', 'INTEGER'
- 'VARCHAR', 'STRING', 'TEXT'
- 'DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ'


```python
@task(task_id='detect_anomalies')
def detect_anomalies(table_name, datetime_col, **kwargs):
    with snowflake.connector.connect(...) as conn:
        ...
        # Data Type and Null checks
        for column, data_type in columns_info:
            print(f'checking {column} of type {data_type}...')
            ...


anomaly_task = detect_anomalies(table_name='{{ dag_run.conf["table_name"] }}',datetime_col='{{ dag_run.conf["datetime_column"] }}')
...
```
`anomaly_task` is how the DAG will later refer to this task when orchestrating tasks.

<a name="numeric"/>

##### 1. Numeric Check
Within the `columns_info` loop, if the data type is 'NUMBER', 'FLOAT', or 'INTEGER' a SQL statement is written designed to select any values that are more than 3 standard deviations away from the mean. Three standard deviations is commonly used as a bound for outliers, but this can be adjusted to fit any use case.

1. Calculate Mean, Standard Deviation, and set z-score threshold `(SELECT AVG(LAT) POP_AVG, STDDEV(LAT) POP_STDDEV, 3 ZSCORE FROM {table_name})`
2. Compute z-score of each row with the expression `({column} - POP_AVG) / POP_STDDEV)`
3. Filter results to include only those with absolute value of z-score greater than three, the value is anomalous due to the value's distance from the mean `WHERE ABS(({column} - POP_AVG) / POP_STDDEV) > ZSCORE;`
4. Conform the final select to use the schema (date_info, column_name, valu, check_type all cast to string types) to allow subsequent checks with various data types to write into the same `dest` table.

```python
            ...
            if data_type in ['NUMBER', 'FLOAT', 'INTEGER']:
                # Numeric Checks
                # Check if column values are >3 standard deviations away from the mean
                query = f"""
                    SELECT TO_VARCHAR({datetime_col}) date_info, '{column}' AS column_name, TO_VARCHAR({column}) AS value, 'ZSCORE > 3' check_type
                    FROM {table_name}
                    JOIN (SELECT AVG(LAT) POP_AVG, STDDEV(LAT) POP_STDDEV, 3 ZSCORE FROM {table_name})
                    WHERE ABS(({column} - POP_AVG) / POP_STDDEV) > ZSCORE;
                """
            ...
```

<a name="categorical"/>

##### 2. Categorical Check
The next stage applies looks at data types of 'VARCHAR', 'STRING', 'TEXT'. Similarly, it writes a SQL statement designed to identify infrequent categorical values which occur less than 5% of the average frequency. In other terms, this checks if a string value occurs significantly less than other values.

1. Calculate the average frequency of each category in `WITH freq AS (...)`
2. Determine a frequency threshold of 5% of the average via `(SELECT AVG(cnt) * 0.05 FROM freq)`
3. Identify categories occuring less than the threshold via `SELECT {column} FROM freq WHERE cnt < (SELECT AVG(cnt) * 0.05 FROM freq)`
4. Conform the final select to match the schema established above (ie: date_info, column_name, valu, check_type all cast to string types)

```python                
            ...
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
            ...
```

<a name="temporal"/>

##### 3. Temporal Check
The next stage is the final check based on data type and applies to temporal data types 'DATE', 'TIMESTAMP', and 'TIMESTAMP_NTZ'. This section writes a simple SQL statement designed to find datetime values outside the expected range, such as in the future or before the year 2000.

And whatever **query** was established, 

```python
            ...
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
            ...
```

<a name="evaldatatypes"/>

##### Evaluate Data Type Checks
Using whatever SQL was written into **query**, create a dataframe with `df_to_snowflake(conn,query,dest)` providing connetion, query, and destination table - which is a method created for this DAG. 

```python
...
report_df = df_to_snowflake(conn,query,dest)
...
```

`df_to_snowflake()` executes the **query** using the provided Snowflake connection and returns the results in a dataframe. The resulting dataframe should be relatively lightweight as it should only contain the outliers of the data, so it can be handled in-memory. 

`write_pandas()` is a method `from snowflake.connector.pandas_tools` used to write the contents of a dataframe into **dest**. The table is created if it does not exist.
```python
...
def df_to_snowflake(conn,query,dest):
    df = conn.cursor().execute(query).fetch_pandas_all()
    if not df.empty:
        write_pandas(conn, df, dest, auto_create_table=True)
    return df
...
```

<a name="nullcheck"/>

##### 4. Null Check
The last column level check is to find columns with more than 5% null values by year-month. The data is selected to match the established schema.

1. `COUNT_IF({column} IS NULL` counts null values in the column.
2. `(COUNT_IF({column} IS NULL) / COUNT(*)) * 100` divides the count of null records by the total number of records, and multiplies it into a percentage.
3. `HAVING null_percentage > 5` filters the resulting data to only show records with more than 5% null values.
4. `SELECT TO_CHAR({datetime_col}, 'YYYY-MM') AS year_month, ...,  ... GROUP BY 1` records are grouped by year-month based on the **datetime**.

If any records are populated in the dataframes created for this null check or the previous data type checks, `reporting = True` is set.

```python
            ...            
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
            ...
```

<a name="rowcount"/>

##### 5. Row Count Check
This is the final check which compares row counts month-over-month to identify any significant changes (ie: >50%, but this threshold can be changed to fit the application).

1. `SELECT TO_CHAR({datetime_col}, 'YYYY-MM') AS year_month, COUNT(*) AS row_count ... GROUP BY 1` counts records by year-month.
2. `row_count_change AS (SELECT year_month, row_count, ...` finds the row count of the current month
2. `LAG(row_count) OVER (ORDER BY year_month) AS prev_row_count` finds the row count of the previous month
3. `(row_count - prev_row_count) / NULLIF(prev_row_count, 0) * 100 AS change_percentage` calculates row count change percent for the current month
4. `QUALIFY ABS(change_percentage) > 50` filters the records to keep those with change percent being more than 50%.
5. The final select conforms the anomalous records to the established schema.

`df_to_snowflake()` is called to evaluate the SQL in Snowflake and write any resulting anomalous data into **dest**. If the resulting dataframe is populated, this can also set `reporting = True`.

When `detect_anomalies` ends, if reporting is true then **dest** is returned and used by the downstream Slack notification.

```python
...
        # Row Count Checks
        # Check if row count change is >50% to previous year-month
        row_count_query = f"""
            WITH row_counts AS (
                SELECT TO_CHAR({datetime_col}, 'YYYY-MM') AS year_month, COUNT(*) AS row_count
                FROM {table_name}
                GROUP BY 1
            ), row_count_change AS (
                SELECT 
                    year_month, 
                    row_count, 
                    LAG(row_count) OVER (ORDER BY year_month) AS prev_row_count,
                    (row_count - prev_row_count) / NULLIF(prev_row_count, 0) * 100 AS change_percentage
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
...
```

<a name="slackalert"/>

#### Send Slack Alerts
The SlackWebhookOperator passes in the Slack connection to send a message to the desired channel(s). The message injects the **dest** returned by `detect_anomalies`.
```python
...
slack_alert_task = SlackWebhookOperator(
        task_id='send_slack_message',
        slack_webhook_conn_id=os.environ['SLACK_CONN'],
        message="Anomalies Found: {{ task_instance.xcom_pull(task_ids='detect_anomalies') }}",
        channel='#airflow-alerts',
    )
...
```

To ensure the slack alert is only triggered if there were anomalies a the @task.branch decorator will decide whether to trigger the slack alert based on whether detect_anomalies returned anything.

And the tasks are ordered from detect_anomalies, branch_task, to send_slack_message.

```python
...
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
```