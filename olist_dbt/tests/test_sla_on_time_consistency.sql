select *
from {{ ref('fct_order_fulfillment') }}
where delivery_sla_status = 'ON_TIME'
  and delivery_delay_days > 0