{{
    config(materialized='table',
    tags=['intermediate','nrr','nrr_table'])
}}

-- with transaction_table as(
--     select 
--         product_id
--         ,EXTRACT(year from payment_month) as year
--         ,revenue
--         ,quantity
--     from {{ ref('stg_transactions_table') }}
-- ),
-- revenue_cal as(
--     select
--         product_id
--         ,year
--         ,sum(revenue * quantity) as yearly_revenue
--     from transaction_table
--     group by 1,2
-- )
-- select
--     * from revenue_cal

-- with month_year_cal as (
--             select
--                 product_id
--                 ,EXTRACT(year from payment_month) as year
--                 ,EXTRACT(month from payment_month) as month
--                 ,revenue
--                 ,quantity
--             from {{ ref('stg_transactions_table') }}
--         )
with exatrct_month_year as (
        select
            product_id
            ,concat(EXTRACT(year from payment_month), '-', LPAD(EXTRACT(month from payment_month)::text, 2, '0')) as month_year
            ,revenue
            ,quantity
        from {{ ref('stg_transactions_table') }}
),
revenue_cal as(
    select
        product_id
        ,month_year
        ,sum(revenue*quantity) as monthly_revenue
    from exatrct_month_year
    group by 1,2
),
revenue_lag as(
    select
        product_id
        ,month_year
        ,monthly_revenue
        ,lag(monthly_revenue,1) over(partition by product_id order by product_id,month_year) as prev_month_revenue
    from revenue_cal
)
select 
    *
from revenue_lag
order by 1,2
