{{ config(materialized='table') }}

WITH fact AS (

    SELECT *
    FROM {{ ref('fact_order_items') }}

),

time_dim AS (

    SELECT *
    FROM {{ ref('time_dim') }}

),

time_metrics AS (

    SELECT
        DATE(order_timestamp) AS order_date,
        order_hour_of_day,

        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_items_sold,
        COUNT(DISTINCT user_id) AS unique_customers,

        AVG(CASE WHEN is_reordered THEN 1 ELSE 0 END) AS reorder_rate,

        COUNT(*)::FLOAT / COUNT(DISTINCT order_id) AS avg_basket_size

    FROM fact
    GROUP BY
        DATE(order_timestamp),
        order_hour_of_day

)

SELECT
    tm.order_date,
    tm.order_hour_of_day,

    td.year,
    td.month,
    td.month_name,
    td.week_of_year,
    td.day_name,
    td.is_weekend,

    tm.total_orders,
    tm.total_items_sold,
    tm.unique_customers,
    tm.reorder_rate,
    tm.avg_basket_size

FROM time_metrics tm
JOIN time_dim td
ON tm.order_date = td.date_day
ORDER BY tm.order_date, tm.order_hour_of_day