use database testdb;
use schema lennar;

CREATE OR REPLACE TABLE user_activity_log (
    user_id VARCHAR COMMENT 'Unique identifier for the user',
    activity_type VARCHAR COMMENT 'Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")',
    activity_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp when the activity occurred',
    product_id VARCHAR COMMENT 'Unique identifier for the product (relevant to activities "view_product", "add_to_cart", "purchase")',
    activity_details VARIANT COMMENT 'Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "pageViews")'
) AS

select 1, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "5"}'
union all select 1, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_B', '{ "price": "1500", "quantity": "2", "pageViews": "5"}'
union all select 1, 'add_to_cart', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2", "pageViews": "8"}'
union all select 2, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "8"}'
union all select 2, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_B', '{ "price": "1500", "quantity": "1", "pageViews": "10"}'
union all select 2, 'add_to_cart', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2", "pageViews": "10"}'
union all select 3, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "11"}'
union all select 3, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "11"}'
union all select 3, 'view_product', current_timestamp(), 'PRODUCT_B', '{ "price": "1500", "quantity": "2", "pageViews": "10"}'
union all select 4, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "1", "pageViews": "10"}'
union all select 4, 'add_to_cart', to_timestamp(current_date()-29), 'PRODUCT_C', '{ "price": "1500", "quantity": "2", "pageViews": "14"}'
union all select 5, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "14"}'
union all select 5, 'add_to_cart', to_timestamp(current_date()-29), 'PRODUCT_A', '{ "price": "1500", "quantity": "2", "pageViews": "14"}'
union all select 6, 'view_product', to_timestamp(current_date()-30), 'PRODUCT_B', '{ "price": "1500", "quantity": "1", "pageViews": "14"}'
union all select 6, 'add_to_cart', to_timestamp(current_date()-29), 'PRODUCT_B', '{ "price": "1500", "quantity": "2", "pageViews": "14"}'
union all select 7, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_A', '{ "price": "1500", "quantity": "1", "pageViews": "17"}'
union all select 7, 'view_product', current_timestamp(), 'PRODUCT_A', '{ "price": "1500", "quantity": "2", "pageViews": "17"}'
union all select 8, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_D', '{ "price": "1500", "quantity": "1", "pageViews": "17"}'
union all select 8, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_B', '{ "price": "1500", "quantity": "2", "pageViews": "17"}'
union all select 9, 'login', to_timestamp(current_date()-60), 'PRODUCT_D', '{ "price": "1500", "quantity": "1", "pageViews": "19"}'
union all select 9, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_D', '{ "price": "1500", "quantity": "1", "pageViews": "19"}'
union all select 9, 'add_to_cart', to_timestamp(current_date()-30), 'PRODUCT_C', '{ "price": "1500", "quantity": "2", "pageViews": "19"}'
union all select 10, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_D', '{ "price": "1500", "quantity": "1", "pageViews": "14"}'
union all select 10, 'add_to_cart', to_timestamp(current_date()-59), 'PRODUCT_C', '{ "price": "1500", "quantity": "2", "pageViews": "14"}'
union all select 11, 'view_product', to_timestamp(current_date()-60), 'PRODUCT_D', '{ "price": "1500", "quantity": "1", "pageViews": "14"}'
union all select 11, 'add_to_cart', to_timestamp(current_date()-29), 'PRODUCT_C', '{ "price": "1500", "quantity": "2", "pageViews": "14"}'
union all select 12, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_C', '{ "price": "1500", "quantity": "2", "pageViews": "5"}'
union all select 13, 'view_product', to_timestamp(current_date()-29), 'PRODUCT_D', '{ "price": "1500", "quantity": "2", "pageViews": "5"}'
;