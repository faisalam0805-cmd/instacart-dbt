{{ config(materialized='table') }}

WITH fact AS (

    SELECT *
    FROM {{ ref('fact_order_items') }}

),

orders AS (

    SELECT *
    FROM {{ ref('orders_stg') }}

),

user_metrics AS (

    SELECT
        user_id,

        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(product_id) AS total_products_purchased,

        COUNT(product_id)::FLOAT / COUNT(DISTINCT order_id) AS avg_basket_size,

        AVG(CASE WHEN is_reordered THEN 1 ELSE 0 END) AS reorder_rate

    FROM fact
    GROUP BY user_id

),

order_behavior AS (

    SELECT
        user_id,
        AVG(days_since_prior_order) AS avg_days_between_orders,
        MAX(order_timestamp) AS last_order_date

    FROM orders
    GROUP BY user_id

)

SELECT
    um.user_id,

    um.total_orders,
    um.total_products_purchased,
    um.avg_basket_size,
    um.reorder_rate,

    ob.avg_days_between_orders,
    CURRENT_DATE - ob.last_order_date::DATE AS days_since_last_order,

    CASE
        WHEN um.total_orders >= 20 THEN 'High Value'
        WHEN um.total_orders >= 10 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment

FROM user_metrics um
JOIN order_behavior ob
ON um.user_id = ob.user_id