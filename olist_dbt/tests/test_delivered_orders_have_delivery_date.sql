select *
from {{ ref('fct_order_fulfillment') }}
where order_status = 'delivered'
  and customer_delivery_date_key is null