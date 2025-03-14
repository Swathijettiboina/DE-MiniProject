{{config (materialized='view', tags=['intermediate','products_rank','products'])}}

with product_revenue as (
    select
        product_id
        ,sum(revenue*quantity) as total_revenue
    from {{ ref('stg_transactions_table') }}
    group by product_id
),
ranked_products as (
    select
        product_id
        ,total_revenue
        ,rank() over (order by total_revenue desc) as product_rank
    from product_revenue
)
select
    *
    from ranked_products
