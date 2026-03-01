SELECT * FROM {{ ref('prior_products_order_stg') }}

UNION ALL

SELECT * FROM {{ ref('train_products_order_stg') }}