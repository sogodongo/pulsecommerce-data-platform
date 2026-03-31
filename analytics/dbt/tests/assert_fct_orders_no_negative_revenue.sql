select
    order_id,
    total_amount_usd,
    net_amount_usd,
    discount_usd,
    status
from {{ ref('fct_orders') }}
where total_amount_usd < 0
   or net_amount_usd   < 0
