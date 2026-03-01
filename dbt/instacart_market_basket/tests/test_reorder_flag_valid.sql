SELECT *
FROM {{ ref('fact_order_items') }}
WHERE is_reordered NOT IN (TRUE, FALSE)