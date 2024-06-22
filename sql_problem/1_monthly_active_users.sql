use database testdb;
use schema lennar;


SELECT DATE_TRUNC('month', activity_timestamp) activity_month, count(distinct user_id) active_user_cnt
FROM user_activity_log
GROUP BY 1
ORDER BY 1 DESC
;