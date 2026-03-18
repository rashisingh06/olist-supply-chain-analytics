with source as (

    select * from {{ source('raw', 'olist_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,

        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_timestamp,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_timestamp,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_timestamp,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_timestamp

    from source

)

select * from renamed