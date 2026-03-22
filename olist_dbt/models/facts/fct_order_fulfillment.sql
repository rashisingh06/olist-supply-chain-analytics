with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

payments as (

    select
        order_id,
        sum(payment_amount) as total_payment_amount,
        min(payment_type) as payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id

),

final as (

    select
        concat(order_items.order_id, '-', order_items.order_item_id) as order_item_id,
        order_items.order_id,
        order_items.order_item_id as order_item_number,

        order_items.product_id,
        order_items.seller_id,
        orders.customer_id as customer_location_id,

        to_number(to_char(cast(orders.order_purchase_timestamp as date), 'YYYYMMDD')) as purchase_date_key,
        to_number(to_char(cast(orders.order_approved_timestamp as date), 'YYYYMMDD')) as approved_date_key,
        to_number(to_char(cast(orders.order_delivered_carrier_timestamp as date), 'YYYYMMDD')) as carrier_delivery_date_key,
        to_number(to_char(cast(orders.order_delivered_customer_timestamp as date), 'YYYYMMDD')) as customer_delivery_date_key,
        to_number(to_char(cast(orders.order_estimated_delivery_timestamp as date), 'YYYYMMDD')) as estimated_delivery_date_key,

        order_items.item_price,
        order_items.freight_amount,
        payments.total_payment_amount as payment_amount,
        payments.payment_type,
        orders.order_status,

        datediff('day', orders.order_purchase_timestamp, orders.order_approved_timestamp) as approval_duration_days,
        datediff('day', orders.order_delivered_carrier_timestamp, orders.order_delivered_customer_timestamp) as shipping_duration_days,
        datediff('day', orders.order_purchase_timestamp, orders.order_delivered_customer_timestamp) as total_lead_time_days,
        datediff('day', orders.order_estimated_delivery_timestamp, orders.order_delivered_customer_timestamp) as delivery_delay_days,

        case
            when orders.order_delivered_customer_timestamp is null
              or orders.order_estimated_delivery_timestamp is null then 'UNKNOWN'
            when datediff('day', orders.order_estimated_delivery_timestamp, orders.order_delivered_customer_timestamp) <= 0 then 'ON_TIME'
            else 'LATE'
        end as delivery_sla_status,

        current_timestamp() as record_created_at

    from order_items
    left join orders
        on order_items.order_id = orders.order_id
    left join payments
        on order_items.order_id = payments.order_id

)

select * from final