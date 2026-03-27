with orders_margin as (
    select * from {{ ref('int_orders_margin') }}
),

shipping as (
    select * from {{ ref('stg_raw__ship') }}
),

final as (
    select
        o.orders_id,
        o.date_date,
        -- log_cost yerine logCost kullanarak formülü düzelttik
        round(
            cast(o.margin as float64) 
            + cast(s.shipping_fee as float64) 
            - (cast(s.logCost as float64) + cast(s.ship_cost as float64))
        , 2) as operational_margin,
        o.quantity,
        o.revenue,
        o.purchase_cost,
        o.margin,
        s.shipping_fee,
        s.logCost, -- Burayı da logCost olarak güncelledik
        s.ship_cost
    from orders_margin o
    left join shipping s 
        using (orders_id)
)

select * from final
order by orders_id desc