SELECT
    department_id,
    LOWER(TRIM(department)) AS department_name
	from {{ source('instacart', 'departments') }}