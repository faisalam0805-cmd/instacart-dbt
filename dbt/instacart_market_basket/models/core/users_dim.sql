{{ config(materialized='table') }}

WITH orders AS (

    SELECT *
    FROM {{ ref('orders_stg') }}

),

user_orders AS (

    SELECT
        user_id,

        COUNT(*) AS total_orders,

        MIN(order_timestamp) AS first_order_date,
        MAX(order_timestamp) AS last_order_date,

        AVG(days_since_prior_order) AS avg_days_between_orders

    FROM orders
    GROUP BY user_id

),

user_preferences AS (

    SELECT
        user_id,

        MODE() WITHIN GROUP (ORDER BY order_hour_of_day) AS most_common_order_hour,

        MODE() WITHIN GROUP (ORDER BY order_dow) AS most_common_order_day

    FROM orders
    GROUP BY user_id

)

SELECT
    uo.user_id,

    uo.total_orders,
    uo.first_order_date,
    uo.last_order_date,
    uo.avg_days_between_orders,

    CURRENT_DATE - uo.last_order_date::DATE AS days_since_last_order,

    CASE
        WHEN uo.total_orders >= 20 THEN 'High Frequency'
        WHEN uo.total_orders >= 10 THEN 'Medium Frequency'
        ELSE 'Low Frequency'
    END AS order_frequency_segment,

    up.most_common_order_hour,
    up.most_common_order_day

FROM user_orders uo
JOIN user_preferences up
ON uo.user_id = up.user_id