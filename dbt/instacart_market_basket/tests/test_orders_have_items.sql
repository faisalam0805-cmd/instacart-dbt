SELECT o.order_id
FROM {{ ref('orders_stg') }} o
LEFT JOIN {{ ref('fact_order_items') }} f
    ON o.order_id = f.order_id
WHERE f.order_id IS NULL
AND o.eval_set = 'prior'