{{ config(materialized='table') }}

WITH products AS (

    SELECT *
    FROM {{ ref('products_stg') }}

),

aisles AS (

    SELECT *
    FROM {{ ref('aisles_stg') }}

),

departments AS (

    SELECT *
    FROM {{ ref('departments_stg') }}

)

SELECT
    p.product_id,

    p.product_name,
    p.product_name_length,

    p.aisle_id,
    a.aisle_name,

    p.department_id,
    d.department_name,

    -- Derived Features
    p.is_organic,
    p.is_frozen,
    CASE
        WHEN p.product_name ILIKE '%store%' THEN TRUE
        ELSE FALSE
    END AS is_private_label,

    ARRAY_LENGTH(STRING_TO_ARRAY(p.product_name, ' '), 1) AS product_name_word_count

FROM products p
JOIN aisles a
    ON p.aisle_id = a.aisle_id
JOIN departments d
    ON p.department_id = d.department_id