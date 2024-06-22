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