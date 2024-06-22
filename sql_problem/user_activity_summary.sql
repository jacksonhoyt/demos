use database testdb;
use schema lennar;

------------------------------------
-- staging monthly activity
------------------------------------
with monthly_active_users as (
    select 
    date_trunc('month', activity_timestamp) month_start
    , to_char(month_start, 'yyyy-mm') as year_month
    , count(distinct user_id) active_user_cnt                       -- count of unique users
    , count(*) total_activity_cnt                                   -- total activity from all users
    , total_activity_cnt/active_user_cnt avg_activities_per_user    -- averaging total activity across users

    from user_activity_log
    group by 1
),

------------------------------------
-- staging retention rate
------------------------------------
first_month_activity as (    
    -- identify user's first active month
    select user_id
    , min(activity_timestamp) first_month_activity_ts
    , to_char(first_month_activity_ts, 'yyyy-mm') first_month_activity_ym

    from user_activity_log
    group by 1
),

second_month_activity as (    
    -- identify user's second active month by excluding activities from user's first month
    select a.user_id
    , min(activity_timestamp) second_month_activity_ts 
    , to_char(second_month_activity_ts, 'yyyy-mm') second_month_activity_ym
    
    from user_activity_log a                                            
    inner join first_month_activity b on a.user_id = b.user_id and extract('month',a.activity_timestamp) != extract('month', b.first_month_activity_ts)
    group by 1
),

user_retention as (    
    ------------------------------------------------------------------------------------------------------------
    -- user_id: a single user
    -- first_month_activity_ym: first year-month of user's activity
    -- second_month_activity_ym: second year-month of user's activity
    -- user_retained_flag: 1=user retained, 0=user not retained
    -- user is retained if second_month_activity_ym is one month after first_month_activity_ym
    -- comments: extra descriptions regarding user retention
    ------------------------------------------------------------------------------------------------------------

    select 
    user_id
    , first_month_activity_ym
    , second_month_activity_ym
    , case when datediff('month', first_month_activity_ts, second_month_activity_ts) = 1 then 1 else 0 end user_retained_flag
    , case 
        when datediff('month', first_month_activity_ts, second_month_activity_ts) > 1 then 'user did not return within a month of first activity' 
        when second_month_activity_ym is null then 'user has not returned since first month of activity'
        else 'user retained, user was active in the month following first activity' 
        end comments

    from first_month_activity a
    left join second_month_activity b using (user_id)
),

user_retention_summary as (
    select first_month_activity_ym, sum(a.user_retained_flag)/count(*) retention_rate
    from user_retention a
    group by 1
),

------------------------------------
-- staging top 3 products
------------------------------------
top_products_by_viewers as (
    select
    to_char(activity_timestamp, 'yyyy-mm') as year_month
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
    select year_month, object_agg(product_id, page_views) as top_products
    from top_products_by_viewers
    group by 1
)

select 
a.year_month
, a.active_user_cnt as MAU
, round(coalesce(b.retention_rate, 0),2) || '%' as retention_rate      -- retention rate = 0 for current month
, a.avg_activities_per_user
, c.top_products

from monthly_active_users a
left join user_retention_summary b on a.year_month = b.first_month_activity_ym
left join top_products_summary c on a.year_month = c.year_month
order by 1 desc
;