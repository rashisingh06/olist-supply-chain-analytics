with customers as (

    select * from {{ ref('stg_customers') }}

)

select
    customer_id as customer_location_id,
    customer_zip_prefix as customer_zip_prefix,
    customer_city,
    customer_state
from customers