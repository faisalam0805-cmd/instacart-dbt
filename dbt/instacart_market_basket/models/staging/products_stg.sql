SELECT
    product_id,
    LOWER(TRIM(product_name)) AS product_name,
    LENGTH(product_name) AS product_name_length,
    case when product_name ilike '%organic%' then true
	else false
	end as is_organic,
	case when product_name ilike '%frozen%' then true
	else false 
	end as is_frozen,
    aisle_id,
    department_id
	from {{ source('instacart', 'products') }}