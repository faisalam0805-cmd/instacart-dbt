select
    order_id,
    {{ classify_order('eval_set') }} as order_type
from {{ ref('orders_stg') }}