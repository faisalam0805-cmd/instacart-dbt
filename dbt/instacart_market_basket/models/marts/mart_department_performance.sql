{{ config(materialized='table') }}

WITH fact AS (

    SELECT *
    FROM {{ ref('fact_order_items') }}

),

products AS (

    SELECT *
    FROM {{ ref('products_dim') }}

),

department_metrics AS (

    SELECT
        p.department_id,
        p.department_name,

        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(*) AS total_items_sold,
        COUNT(DISTINCT f.user_id) AS unique_customers,

        SUM(CASE WHEN f.is_reordered THEN 1 ELSE 0 END) AS total_reorders,

        AVG(CASE WHEN f.is_reordered THEN 1 ELSE 0 END) AS reorder_rate,

        AVG(f.add_to_cart_order) AS avg_cart_position,

        COUNT(*)::FLOAT / COUNT(DISTINCT f.order_id) AS avg_basket_size

    FROM fact f
    JOIN products p
        ON f.product_id = p.product_id

    GROUP BY
        p.department_id,
        p.department_name

),

total_sales AS (

    SELECT SUM(total_items_sold) AS overall_sales
    FROM department_metrics

)

SELECT
    dm.*,

    dm.total_items_sold::FLOAT / ts.overall_sales AS department_sales_share,

    RANK() OVER (ORDER BY dm.total_items_sold DESC) AS department_rank

FROM department_metrics dm
CROSS JOIN total_sales ts
ORDER BY department_rank