use database testdb;
use schema lennar;

with product_views as (
    select 
    extract('year',activity_timestamp) activity_year
    ,extract('month',activity_timestamp) activity_month
    -- ,date_trunc('month',activity_timestamp) activity_month
    -- , activity_type
    , product_id
    , sum(activity_details:pageViews::int) page_views
    , count(distinct user_id) unique_user_view_cnt
    , rank() over (order by activity_month DESC, product_id, unique_user_view_cnt DESC) rnk_overall
    , rank() over (partition by activity_month order by activity_month DESC, product_id, unique_user_view_cnt DESC, page_views) rnk_by_month
    , rank() over (order by page_views DESC) rnk_page_views
    
    from user_activity_log
    -- where activity_type = 'view_product'
    group by 1,2,3
)

-- use rnk_by_month to see top N products by the number of unique users who viewed them
select *
from product_views
-- where rnk_by_month <= 3
order by activity_month desc, rnk_by_month
;