SELECT
    aisle_id,
    LOWER(TRIM(aisle)) AS aisle_name
	from {{ source('instacart', 'aisles') }}