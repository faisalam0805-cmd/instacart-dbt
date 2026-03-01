{{ config(materialized='table') }}

WITH fact AS (

    SELECT *
    FROM {{ ref('fact_order_items') }}

),

products AS (

    SELECT *
    FROM {{ ref('products_dim') }}

),

product_metrics AS (

    SELECT
        product_id,

        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_quantity_sold,
        COUNT(DISTINCT user_id) AS unique_customers,

        SUM(CASE WHEN is_reordered THEN 1 ELSE 0 END) AS total_reorders,

        AVG(CASE WHEN is_reordered THEN 1 ELSE 0 END) AS reorder_rate,

        AVG(add_to_cart_order) AS avg_cart_position

    FROM fact
    GROUP BY product_id

)

SELECT
    pm.product_id,

    p.product_name,
    p.aisle_name,
    p.department_name,

    pm.total_orders,
    pm.total_quantity_sold,
    pm.unique_customers,

    pm.total_reorders,
    pm.reorder_rate,

    pm.avg_cart_position,

    RANK() OVER (ORDER BY pm.total_quantity_sold DESC) AS product_rank

FROM product_metrics pm
JOIN products p
ON pm.product_id = p.product_id