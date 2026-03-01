{{ config(materialized='table') }}

WITH order_products AS (

    SELECT * FROM {{ ref('int_all_order_products') }}

),

orders AS (

    SELECT * FROM {{ ref('orders_stg') }}

),

products AS (

    SELECT * FROM {{ ref('products_stg') }}

),

aisles AS (

    SELECT * FROM {{ ref('aisles_stg') }}

),

departments AS (

    SELECT * FROM {{ ref('departments_stg') }}

)

SELECT

    -- Keys
    op.order_id,
    o.user_id,
    op.product_id,

    -- Order Attributes
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,
    o.order_timestamp,

    -- Product Attributes
    p.product_name,
    a.aisle_name,
    d.department_name,

    -- Behavior Metrics
    op.add_to_cart_order,
    op.cart_position_bucket,
    op.is_reordered

FROM order_products op

JOIN orders o
    ON op.order_id = o.order_id

JOIN products p
    ON op.product_id = p.product_id

JOIN aisles a
    ON p.aisle_id = a.aisle_id

JOIN departments d
    ON p.department_id = d.department_id