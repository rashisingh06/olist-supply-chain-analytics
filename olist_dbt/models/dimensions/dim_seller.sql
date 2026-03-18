with sellers as (

    select * from {{ ref('stg_sellers') }}

)

select
    seller_id,
    seller_zip_prefix,
    seller_city,
    seller_state
from sellers