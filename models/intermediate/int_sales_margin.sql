with sales as (
    -- Staging sales modeline atıf
    select * from {{ ref('stg_raw__sales') }}
),

product as (
    -- Staging product modeline atıf
    select * from {{ ref('stg_raw__product') }}
),

final as (
    select
        sales.*,
        product.purchase_price,
        -- 1. Satın Alma Maliyeti Hesabı: Adet * Birim Alış Fiyatı
        round(cast(sales.quantity as float64) * cast(product.purchase_price as float64), 2) as purchase_cost,
        -- 2. Marj Hesabı: Ciro - Satın Alma Maliyeti
        round(cast(sales.revenue as float64) - (cast(sales.quantity as float64) * cast(product.purchase_price as float64)), 2) as margin
    from sales
    left join product 
        on sales.products_id = product.products_id
)

select * from final