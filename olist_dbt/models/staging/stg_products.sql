with source as (

    select * from {{ source('raw', 'olist_products') }}

),

renamed as (

    select
        product_id,
        product_category_name,
        product_name_lenght as product_name_length,
        product_description_lenght as product_description_length,
        product_photos_qty as product_photos_quantity,
        product_weight_g as product_weight_grams,
        product_length_cm,
        product_height_cm,
        product_width_cm

    from source

)

select * from renamed