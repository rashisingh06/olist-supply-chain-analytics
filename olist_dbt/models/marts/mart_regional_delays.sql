with regional_metrics as (

    select
        dcl.customer_state,
        dcl.customer_city,
        count(*) as total_orders,
        avg(f.total_lead_time_days) as avg_lead_time_days,
        avg(f.delivery_delay_days) as avg_delivery_delay_days

    from {{ ref('fct_order_fulfillment') }} f
    left join {{ ref('dim_customer_location') }} dcl
        on f.customer_location_id = dcl.customer_location_id

    group by
        dcl.customer_state,
        dcl.customer_city

),

final as (

    select
        customer_state,
        customer_city,
        total_orders,
        round(avg_lead_time_days, 2) as avg_lead_time_days,
        round(avg_delivery_delay_days, 2) as avg_delivery_delay_days,
        case
            when avg_delivery_delay_days <= 0 then 'On Time'
            when avg_delivery_delay_days <= 2 then 'Slight Delay'
            else 'High Delay'
        end as regional_delay_category

    from regional_metrics

)

select * from final