Select 
    order_id,
	user_id,
	eval_set,
	order_number,
    order_dow,
	order_hour_of_day,
	days_since_prior_order,
-- derieved fields
	case when order_number = 1 then True
	else false 
	End As is_first_order,
	case when order_number > 1 then true
	Else false
	End as is_returning_customer,
	case when order_dow in (0,6) then true
	else false
	end as is_weekend,
	CASE
        WHEN order_hour_of_day BETWEEN 5 AND 11 THEN 'Morning'
        WHEN order_hour_of_day BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN order_hour_of_day BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS order_time_bucket,
	CASE
        WHEN days_since_prior_order <= 7 THEN 'Frequent'
        WHEN days_since_prior_order <= 30 THEN 'Regular'
        ELSE 'Occasional'
    END AS order_frequency_segment,
    CASE
        WHEN days_since_prior_order <= 7 THEN 'Recent'
        WHEN days_since_prior_order <= 30 THEN 'Medium'
        ELSE 'Dormant'
    END AS recency_bucket,
	--creating a pseudo timeline
	DATE '2020-01-01'
      + (
        SUM(COALESCE(days_since_prior_order,0))
        OVER (
        PARTITION BY user_id
        ORDER BY order_number
              )
         ) * INTERVAL '1 day'
      + (order_hour_of_day * INTERVAL '1 hour')
      AS order_timestamp
from {{ source('instacart', 'orders') }}
