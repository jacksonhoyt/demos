# SQL PROBLEM (SNOWFLAKE)
Given a Snowflake table named user_activity_log with the given schema. DDL statement provides mock data for testing.

Table Name: `user_activity_log`
column|description
---|---
user_id|Unique identifier for the user
activity_type|Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")
activity_timestamp|Timestamp when the activity occurred
product_id|Unique identifier for the product (relevant for "view_product", "add_to_cart", "purchase" activities)
activity_details|Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "page views")


## QUESTION IN FOUR PARTS

PARTS|DESCRIPTION
---|---
Monthly Active Users | The number of unique users who have performed any activity in each month. Calculate the MAU for each month within the dataset. Use DATE_TRUNC to aggregate activities by month.
Retention Rate | The percentage of new users in a given month who return and perform any activity in the following month. Determine the retention rate by identifying users who were new in one month and then calculating the percentage of these users who had any activity in the subsequent month.
Average Monthly Activities per User | The average number of activities performed by active users in each month. Calculate the average number of activities per active user for each month. Group by month and divide the total number of activities by the number of unique users.
Top 3 Popular Products by Views | Identify the top 3 products each month by the number of unique users who viewed them. Using the activity_details field, extract the product views and rank the top 3 products for each month based on the number of unique viewers.

#### Expected Output
Your query should return a monthly breakdown with the following columns
COLUMN|DESCRIPTION
---|---
year_month|The month and year for the analysis
MAU|Monthly Active Users count
retention_rate|Retention rate percentage
avg_activities_per_user|Average number of activities per active user
top_products | A list (or separate columns) for the top 3 products by views, along with their view counts

# SOLUTION
##### EXAMPLE OUTPUT
###### (using sample data in `./user_activity_log_DDL.sql`)
This is the output provided by the SQL in the Solution file
YEAR_MONTH|MAU|RETENTION_RATE|AVG_ACTIVITIES_PER_USER|TOP_PRODUCTS
---|---|---|---|---
2024-06|4|0.00%|1.000000|PRODUCT_B:10, PRODUCT_A:17
2024-05|11|0.38%|1.545455|PRODUCT_A:49, PRODUCT_B:29, PRODUCT_C:15
2024-04|5|0.60%|1.400000|PRODUCT_D:64, PRODUCT_A:17

#### SQL 
##### `./user_activity_summary.sql`
```sql
use database testdb;
use schema lennar;

------------------------------------
-- staging monthly activity
------------------------------------
with monthly_active_users_summary as (
    select 
    date_trunc('month', activity_timestamp) month_start
    , to_char(month_start, 'yyyy-mm') year_month
    , count(distinct user_id) active_user_cnt                       -- count of unique users
    , count(*) total_activity_cnt                                   -- total activity from all users
    , total_activity_cnt/active_user_cnt avg_activities_per_user    -- averaging total activity across users

    from user_activity_log
    group by 1
),

------------------------------------
-- staging retention rate
------------------------------------
first_activity as (    
    -- identify user's first active month by their earliest activity
    select user_id, min(activity_timestamp) first_activity_ts
    from user_activity_log
    group by 1
),

second_month_activity as (    
    -- identify user's second active month by excluding activities from user's first month
    select a.user_id, min(activity_timestamp) second_month_activity_ts 
    from user_activity_log a                                            
    inner join first_month_activity b on a.user_id = b.user_id and extract('month',a.activity_timestamp) != extract('month', b.first_activity_ts)
    group by 1
),

user_retention_summary as (
    select 
    to_char(first_activity_ts, 'yyyy-mm') year_month
    , sum(case when datediff('month', first_activity_ts, second_month_activity_ts) = 1 then 1 else 0 end)/count(*) as retention_rate

    from first_activity a
    left join second_month_activity b using (user_id)
    group by 1
),

------------------------------------
-- staging top 3 products
------------------------------------
top_products_by_viewers as (
    select
    to_char(activity_timestamp, 'yyyy-mm') year_month
    , product_id
    , count(distinct user_id) as unique_viewers
    , rank() over (partition by year_month order by unique_viewers desc) rnk
    , sum(activity_details:pageViews::int) page_views
    
    from user_activity_log
    where activity_type = 'view_product'
    group by 1,2
    qualify rnk <= 3
),

top_products_summary as (
    -- top_products: returns [PRODUCT ID]:[PAGE VIEWS] sorted by unique viewers
    select year_month, listagg(product_id || ':' || page_views, ', ') within group (order by rnk) top_products
    from top_products_by_viewers
    group by 1
)

select 
a.year_month
, a.active_user_cnt as MAU
, round(coalesce(b.retention_rate, 0),2) || '%' as retention_rate      -- retention rate = 0 for current month
, a.avg_activities_per_user
, c.top_products

from monthly_active_users_summary a
left join user_retention_summary b using (year_month)
left join top_products_summary c using (year_month)
order by 1 desc
;
```