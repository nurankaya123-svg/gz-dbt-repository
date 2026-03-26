with orders_margin as (
    -- Bir önceki intermediate modeline atıf yapıyoruz
    select * from {{ ref('int_sales_margin') }}
),

final as (
    select
        orders_id,
        date_date,
        -- Sipariş bazında toplam ciro, maliyet ve marjı topluyoruz
        round(sum(revenue), 2) as revenue,
        round(sum(quantity), 2) as quantity,
        round(sum(purchase_cost), 2) as purchase_cost,
        round(sum(margin), 2) as margin
    from orders_margin
    group by 
        orders_id, 
        date_date
    order by orders_id desc    
)

select * from final