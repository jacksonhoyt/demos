use database testdb;
use schema lennar;


with irst_month_activity as (    
    -- identify user's first active month
    select user_id
    , min(activity_timestamp) first_month_activity_ts
    , to_varchar(date_trunc('month',first_month_activity_ts), 'yyyy-mm') first_month_activity_ym

    from user_activity_log
    group by 1
),

second_month_activity as (    
    -- identify user's second active month
    -- by excluding activities from user's first month
    select a.user_id
    , min(activity_timestamp) second_month_activity_ts 
    , to_varchar(date_trunc('month',second_month_activity_ts), 'yyyy-mm') second_month_activity_ym
    
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


select first_month_activity_ym, sum(a.user_retained_flag)/count(*) retention_rate
from user_retention a
group by 1
order by 1
