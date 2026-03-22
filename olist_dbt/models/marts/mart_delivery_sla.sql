with sla_metrics as (

    select
        delivery_sla_status,
        count(*) as total_orders,
        avg(total_lead_time_days) as avg_lead_time_days,
        avg(delivery_delay_days) as avg_delivery_delay_days,
        sum(item_price) as total_revenue

    from {{ ref('fct_order_fulfillment') }}
    group by delivery_sla_status

),

final as (

    select
        delivery_sla_status,
        total_orders,
        round(avg_lead_time_days, 2) as avg_lead_time_days,
        round(avg_delivery_delay_days, 2) as avg_delivery_delay_days,
        round(total_revenue, 2) as total_revenue

    from sla_metrics

)

select * from final