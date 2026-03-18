with products as (

    select * from {{ ref('stg_products') }}

),

translations as (

    select * from {{ ref('stg_product_category_translation') }}

),

final as (

    select
        p.product_id,
        p.product_category_name,
        t.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_quantity,
        p.product_weight_grams,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm

    from products p
    left join translations t
        on p.product_category_name = t.product_category_name

)

select * from final
