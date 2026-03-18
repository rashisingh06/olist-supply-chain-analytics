with source as (

    select * from {{ source('raw', 'olist_order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_timestamp,
        cast(price as numeric(10,2)) as item_price,
        cast(freight_value as numeric(10,2)) as freight_amount

    from source

)

select * from renamed