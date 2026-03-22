select *
from {{ ref('fct_order_fulfillment') }}
where total_lead_time_days < 0