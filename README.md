# SQL PROBLEM OVERVIEW (SNOWFLAKE)
Using the provided info as a starting point, solve the questions below using SQL.

Given a Snowflake table named user_activity_log with the given schema. DDL statement provides mock data for testing.

Table Name: `user_activity_log`
column|description
---|---
user_id|Unique identifier for the user
activity_type|Type of activity (e.g. "login", "purchase", "view_product", "add_to_cart")
activity_timestamp|Timestamp when the activity occurred
product_id|Unique identifier for the product (e.g. "view_product", "add_to_cart", "purchase")
activity_details|Semi-structured JSON containing additional information about the activity (e.g., "price", "quantity", "page views")


# QUESTIONS
Question parameters are outlined in DETAILS

QUESTION|SOLUTION FILE
---|---
Monthly Active Users | `./1_monthly_active_users.sql`
Retention Rate | `./2_retention_rate.sql`
Average Monthly Activities per User | `./3_average_monthly_activities_per_user.sql`
Top 3 Popular Products by Views | `./4_top_three_products_by_views.sql`

# DETAILS
## 1. Monthly Active Users (MAU)
The number of unique users who have performed any activity in each month. 
Calculate the MAU for each month within the dataset. 
Use DATE_TRUNC to aggregate activities by month.

### Sample data provided to user_activity_log
Sample data shown here is from user_activity_log DDL provided in Solution file comments
ACTIVITY_TIMESTAMP|USER_ID
---|---
2024-***04***-20T00:00:00.000Z|7
2024-***04***-20T00:00:00.000Z|8
2024-***04***-20T00:00:00.000Z|9
2024-***05***-20T00:00:00.000Z|1
2024-***05***-20T00:00:00.000Z|2
2024-***05***-20T00:00:00.000Z|3
2024-***05***-20T00:00:00.000Z|4
2024-***05***-20T00:00:00.000Z|4
2024-***05***-20T00:00:00.000Z|5
2024-***05***-20T00:00:00.000Z|5
2024-***05***-20T00:00:00.000Z|6
2024-***05***-20T00:00:00.000Z|6
2024-***05***-20T00:00:00.000Z|8
2024-***05***-20T00:00:00.000Z|9
2024-***05***-21T00:00:00.000Z|1
2024-***05***-21T00:00:00.000Z|2
2024-***05***-21T00:00:00.000Z|3
2024-***06***-19T20:23:48.554Z|1
2024-***06***-19T20:23:48.554Z|2
2024-***06***-19T20:23:48.554Z|3
2024-***06***-19T20:23:48.554Z|7

### Sample data output
This is the output provided by the SQL in the Solution file
ACTIVITY_MONTH|MONTHLY_ACTIVE_USER_CNT
---|---
2024-06-01T00:00:00.000Z|4
2024-05-01T00:00:00.000Z|8
2024-04-01T00:00:00.000Z|3


## 2. Retention Rate
The percentage of new users in a given month who return and perform any activity in the following month. 
Determine the retention rate by identifying users who were new in one month and then calculating the percentage of these users who had any activity in the subsequent month.

### Sample data provided to user_activity_log
Sample data shown here is from user_activity_log DDL provided in Solution file comments
USER_ID|ACTIVITY_TIMESTAMP
---|---
7|2024-04-20T00:00:00.000Z
8|2024-04-20T00:00:00.000Z
9|2024-04-20T00:00:00.000Z
1|2024-05-20T00:00:00.000Z
2|2024-05-20T00:00:00.000Z
3|2024-05-20T00:00:00.000Z
4|2024-05-20T00:00:00.000Z
5|2024-05-20T00:00:00.000Z
6|2024-05-20T00:00:00.000Z
8|2024-05-20T00:00:00.000Z
9|2024-05-20T00:00:00.000Z
1|2024-06-19T19:17:46.690Z
2|2024-06-19T19:17:46.690Z
3|2024-06-19T19:17:46.690Z
7|2024-06-19T19:17:46.690Z

### Sample data output
This is the output provided by the SQL in the Solution file
FIRST_ACTIVE_MONTH|RETENTION_RATE
---|---
2024-04-20T00:00:00.000Z|0.666667
2024-05-20T00:00:00.000Z|0.500000


## 3. Average Monthly Activities per User
The average number of activities performed by active users in each month. 
Calculate the average number of activities per active user for each month. 
Group by month and divide the total number of activities by the number of unique users.

### Sample data provided to user_activity_log
Sample data shown here is from user_activity_log DDL provided in Solution file comments
USER_ID|ACTIVITY_TIMESTAMP|ACTIVITY_TYPE
---|---|---
1|2024-05-20T00:00:00.000Z|view_product
1|2024-05-21T00:00:00.000Z|view_product
1|2024-06-19T20:23:48.554Z|add_to_cart
2|2024-05-20T00:00:00.000Z|view_product
2|2024-05-21T00:00:00.000Z|view_product
2|2024-06-19T20:23:48.554Z|add_to_cart
3|2024-05-20T00:00:00.000Z|view_product
3|2024-05-21T00:00:00.000Z|view_product
3|2024-06-19T20:23:48.554Z|add_to_cart
4|2024-05-20T00:00:00.000Z|view_product
4|2024-05-20T00:00:00.000Z|add_to_cart
5|2024-05-20T00:00:00.000Z|view_product
5|2024-05-20T00:00:00.000Z|add_to_cart
6|2024-05-20T00:00:00.000Z|view_product
6|2024-05-20T00:00:00.000Z|add_to_cart
7|2024-04-20T00:00:00.000Z|view_product
7|2024-06-19T20:23:48.554Z|add_to_cart
8|2024-04-20T00:00:00.000Z|view_product
8|2024-05-20T00:00:00.000Z|add_to_cart
9|2024-04-20T00:00:00.000Z|view_product
9|2024-05-20T00:00:00.000Z|add_to_cart

### Sample data output
This is the output provided by the SQL in the Solution file
FIRST_ACTIVE_MONTH|RETENTION_RATE
---|---
2024-04-20T00:00:00.000Z|0.666667


## 4. Top 3 Popular Products by Views
### Question
Identify the top 3 products each month by the number of unique users who viewed them. 
Using the activity_details field, extract the product views and rank the top 3 products for each month based on the number of unique viewers.

### Sample data provided to user_activity_log
Sample data shown here is from user_activity_log DDL provided in Solution file comments



### Sample data output
This is the output provided by the SQL in the Solution file



# EXPECTED OUTPUT
Your query should return a monthly breakdown with the following columns:
- year_month: The month and year for the analysis.
- MAU: Monthly Active Users count.
- retention_rate: Retention rate percentage.
- avg_activities_per_user: Average number of activities per active user.
- top_products: A list (or separate columns) for the top 3 products by views, along with
their view counts.