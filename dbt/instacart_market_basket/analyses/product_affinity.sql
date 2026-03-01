SELECT
    p1.product_name AS product_a,
    p2.product_name AS product_b,
    COUNT(*) AS co_purchase_count
FROM {{ ref('fact_order_items') }} o1
JOIN {{ ref('fact_order_items') }} o2
    ON o1.order_id = o2.order_id
    AND o1.product_id < o2.product_id
JOIN {{ ref('products_dim') }} p1 ON o1.product_id = p1.product_id
JOIN {{ ref('products_dim') }} p2 ON o2.product_id = p2.product_id
GROUP BY 1,2
ORDER BY co_purchase_count DESC
LIMIT 20;