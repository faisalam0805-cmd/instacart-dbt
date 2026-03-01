{{ config(materialized='table') }}

WITH dates AS (

    SELECT DISTINCT
        DATE(order_timestamp) AS date_day
    FROM {{ ref('orders_stg') }}

)

SELECT
    date_day,

    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,

    TO_CHAR(date_day, 'Month') AS month_name,

    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(WEEK FROM date_day) AS week_of_year,

    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DOW FROM date_day) AS day_of_week,

    TO_CHAR(date_day, 'Day') AS day_name,

    CASE
        WHEN EXTRACT(DOW FROM date_day) IN (0,6)
        THEN TRUE ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN date_day = DATE_TRUNC('month', date_day)
        THEN TRUE ELSE FALSE
    END AS is_month_start,

    CASE
        WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE
        THEN TRUE ELSE FALSE
    END AS is_month_end

FROM dates
ORDER BY date_day