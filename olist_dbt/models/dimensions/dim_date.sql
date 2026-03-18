with dates as (

    select order_purchase_timestamp as ts from {{ ref('stg_orders') }}
    union
    select order_approved_timestamp as ts from {{ ref('stg_orders') }}
    union
    select order_delivered_carrier_timestamp as ts from {{ ref('stg_orders') }}
    union
    select order_delivered_customer_timestamp as ts from {{ ref('stg_orders') }}
    union
    select order_estimated_delivery_timestamp as ts from {{ ref('stg_orders') }}

),

cleaned as (

    select distinct cast(ts as date) as calendar_date
    from dates
    where ts is not null

)

select
    to_number(to_char(calendar_date, 'YYYYMMDD')) as date_key,
    calendar_date,
    year(calendar_date) as year,
    quarter(calendar_date) as quarter,
    month(calendar_date) as month,
    monthname(calendar_date) as month_name,
    day(calendar_date) as day,
    dayofweek(calendar_date) as day_of_week,
    dayname(calendar_date) as day_name
from cleaned