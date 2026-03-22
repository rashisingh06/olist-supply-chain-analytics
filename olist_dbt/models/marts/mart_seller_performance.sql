with seller_metrics as (

    select
        f.seller_id,
        s.seller_city,
        s.seller_state,
        count(*) as total_orders,
        avg(f.total_lead_time_days) as avg_lead_time_days,
        avg(f.delivery_delay_days) as avg_delivery_delay_days,
        sum(f.item_price) as total_revenue

    from {{ ref('fct_order_fulfillment') }} f
    left join {{ ref('dim_seller') }} s
        on f.seller_id = s.seller_id

    group by
        f.seller_id,
        s.seller_city,
        s.seller_state

),

final as (

    select
        seller_id,
        seller_city,
        seller_state,
        total_orders,
        round(avg_lead_time_days, 2) as avg_lead_time_days,
        round(avg_delivery_delay_days, 2) as avg_delivery_delay_days,
        round(total_revenue, 2) as total_revenue,
        case
            when avg_delivery_delay_days <= 0 then 'On Time'
            when avg_delivery_delay_days <= 2 then 'Slight Delay'
            else 'High Delay'
        end as delay_category

    from seller_metrics

)

select * from final