use database testdb;
use schema lennar;

CREATE OR REPLACE TABLE user_activity_log (
    user_id VARCHAR COMMENT 'Unique identifier for the user',
    activity_type VARCHAR COMMENT 'Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")',
    activity_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp when the activity occurred',
    product_id VARCHAR COMMENT 'Unique identifier for the product (e.g. "view_product", "add_to_cart", "purchase")',
    activity_details VARIANT COMMENT 'Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "page views")'
) AS

select 1, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 1, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 2, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 2, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 3, 'view_product', to_timestamp(current_date()-30), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 3, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 4, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
union all select 5, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
union all select 6, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 7, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 7, 'add_to_cart', current_timestamp(), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 8, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 8, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'

union all select 9, 'view_product', to_timestamp(current_date()-60), 'view_product', '{ "price": "1500", "quantity": "1" }'
union all select 9, 'add_to_cart', to_timestamp(current_date()-30), 'add_to_cart', '{ "price": "1500", "quantity": "2" }'
;