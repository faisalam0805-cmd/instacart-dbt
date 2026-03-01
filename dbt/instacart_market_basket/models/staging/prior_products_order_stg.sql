SELECT
    order_id,
    product_id,

    add_to_cart_order,

    case when reordered = 1 then true 
    else false
    end as is_reordered,
    CASE
        WHEN add_to_cart_order = 1 THEN 'first_item'
        WHEN add_to_cart_order <= 5 THEN 'top_5'
        WHEN add_to_cart_order <= 10 THEN 'top_10'
        ELSE 'later_added'
    END AS cart_position_bucket
	from {{ source('instacart', 'prior_products_order') }}