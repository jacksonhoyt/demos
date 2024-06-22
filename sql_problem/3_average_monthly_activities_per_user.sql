
use database testdb;
use schema lennar;

select 
DATE_TRUNC( 'month', activity_timestamp) activity_month_ts
, count(distinct user_id) user_cnt                          -- count of unique users
, count(*) total_activity_cnt                               -- total activity from all users
, total_activity_cnt/user_cnt avg_activities_per_user       -- averaging total activity across users

from user_activity_log
group by 1 
order by 1 desc