use database testdb;
use schema lennar;


WITH user_activity AS (    
    SELECT DISTINCT user_id, activity_timestamp
    FROM user_activity_log
),

new_users_first_month AS (    
    SELECT user_id, MIN(activity_timestamp) first_active_month
    FROM user_activity
    GROUP BY 1
),

new_users_second_month AS (    
    SELECT a.user_id, MIN(activity_timestamp) second_active_month
    FROM user_activity a
    INNER JOIN new_users_first_month b ON a.user_id = b.user_id AND EXTRACT('month',a.activity_timestamp) != EXTRACT('month', b.first_active_month)
    GROUP BY 1
),

user_retention AS (    
    -- description:
    -- user [VARCHAR]
    -- user's first active month [VARCHAR]
    -- user's second active month [VARCHAR]
    -- user is retained if second_active_month is one month after first_active_month, 1=RETAINED, 0=NOT RETAINED [INT]

    SELECT 
    user_id
    , first_active_month
    , second_active_month
    , case when datediff('month', first_active_month, second_active_month) = 1 then 1 else 0 end user_retained_flag

    FROM new_users_first_month a
    LEFT JOIN new_users_second_month b USING (user_id)
)

-- results read as:
-- XX.X% of users (retention_rate) who had their first activity in Month X (first_active_month) 
-- also showed activity in the subsequent month
SELECT a.first_active_month, sum(a.user_retained_flag)/count(*) retention_rate
FROM user_retention a
WHERE first_active_month is not null
GROUP BY 1
ORDER BY 1
;






-- TESTED ON FOLLOWING DDL
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-- use database testdb;
-- use schema lennar;

-- CREATE OR REPLACE TABLE user_activity_log (
--     user_id VARCHAR COMMENT 'Unique identifier for the user',
--     activity_type VARCHAR COMMENT 'Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")',
--     activity_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp when the activity occurred',
--     product_id VARCHAR COMMENT 'Unique identifier for the product (relevant to activities "view_product", "add_to_cart", "purchase")',
--     activity_details VARIANT COMMENT 'Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "page views")'
-- ) AS

-- select 1, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1" }'
-- union all select 1, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_A', '{ "price": "1500", "quantity": "2" }'
-- union all select 1, 'add_to_cart', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2" }'

-- union all select 2, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1" }'
-- union all select 2, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_B', '{ "price": "1500", "quantity": "1" }'
-- union all select 2, 'add_to_cart', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2" }'

-- union all select 3, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1" }'
-- union all select 3, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_B', '{ "price": "1500", "quantity": "1" }'
-- union all select 3, 'add_to_cart', current_timestamp(), 'PRODUCT_B', '{ "price": "1500", "quantity": "2" }'

-- union all select 4, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "1" }'
-- union all select 4, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "2" }'

-- union all select 5, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "1" }'
-- union all select 5, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "2" }'

-- union all select 6, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_B', '{ "price": "1500", "quantity": "1" }'
-- union all select 6, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_B', '{ "price": "1500", "quantity": "2" }'

-- union all select 7, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_A', '{ "price": "1500", "quantity": "1" }'
-- union all select 7, 'add_to_cart', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2" }'

-- union all select 8, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_B', '{ "price": "1500", "quantity": "1" }'
-- union all select 8, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_B', '{ "price": "1500", "quantity": "2" }'

-- union all select 9, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_C', '{ "price": "1500", "quantity": "1" }'
-- union all select 9, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "2" }'
-- ;





















------------------------------------ 
-- use database testdb;
-- use schema lennar;

-- CREATE OR REPLACE TABLE user_activity_log (
--     user_id VARCHAR COMMENT 'Unique identifier for the user',
--     activity_type VARCHAR COMMENT 'Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")',
--     activity_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp when the activity occurred',
--     product_id VARCHAR COMMENT 'Unique identifier for the product (e.g. "view_product", "add_to_cart", "purchase")',
--     activity_details VARIANT COMMENT 'Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "page views")'
-- ) AS

-- select 1, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 1, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 2, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 2, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 3, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 3, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 4, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
-- union all select 5, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
-- union all select 6, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 7, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 7, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 8, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 8, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

-- union all select 9, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
-- union all select 9, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
-- ;