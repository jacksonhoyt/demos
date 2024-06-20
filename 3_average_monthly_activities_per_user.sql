use database testdb;
use schema lennar;

with user_activity as (
    select user_id, DATE_TRUNC('month',activity_timestamp) activity_month, count(activity_type) activity_cnt
    from user_activity_log
    group by 1, 2
),

avg_monthly_activity as (
    select activity_month, avg(activity_cnt) avg_activity_cnt
    from user_activity
    group by 1
    order by 1 desc
),

user_avg_monthly_activity as (
    select activity_month, user_id, avg(activity_cnt) avg_activity_cnt
    from user_activity
    group by 1, 2
    order by 1 desc, 2
),

activities_per_unique_user as (
    select activity_month
    , sum(activity_cnt) total_activity
    , count(distinct user_id) unique_active_users
    , total_activity/unique_active_users total_activity_per_unique_user
    from user_activity
    group by 1
    order by 1 desc
)

select * from activities_per_unique_user
;