{{
    config(materialized='table',
    tags=['intermediate','metrics'])
}}
with formate_date as(
    select
        t.customer_id
        ,t.product_id
        ,concat(EXTRACT(year from payment_month), '-', LPAD(EXTRACT(month from payment_month)::text, 2, '0')) as month_year
        ,t.revenue_type
        ,t.revenue
        ,t.quantity
        ,t.companies
    from {{ ref('stg_transactions_table') }} t
),
cal_total_revenue as(
    select
        customer_id
        ,product_id
        ,month_year
        ,sum(revenue*quantity) as total_revenue
    from formate_date
    group by 1,2,3
),
first_last_order_dates_for_products as (
    select
        product_id
        ,min(payment_month) as first_order_date_for_product
        ,max(payment_month) as last_order_date_for_product
    from {{ ref('stg_transactions_table') }}
    group by product_id
),
first_last_order_dates_for_customers as (
    select
        customer_id
        ,min(payment_month) as first_order_date_for_customer
        ,max(payment_month) as last_order_date_for_customer
    from {{ ref('stg_transactions_table') }}
    group by customer_id
),
join_revenu_customers  as(
    select 
        c.customer_id
        ,c.product_id
        ,c.month_year
        ,c.total_revenue
        ,f.first_order_date_for_customer
        ,f.last_order_date_for_customer
    from cal_total_revenue c
    left join first_last_order_dates_for_customers f
    on c.customer_id = f.customer_id
),
union_tables as(
    select
        j.*
        ,p.first_order_date_for_product
        ,p.last_order_date_for_product

    from join_revenu_customers j
    left join first_last_order_dates_for_products p
    on j.product_id = p.product_id
),
cal_cross_sell_revenue as(
    select
        *
        ,case 
            when first_order_date_for_product > first_order_date_for_customer 
            then total_revenue
            else 0
        end as cross_sell_revenue
    from union_tables
),
join_products_table as(
    select
        c.*
        ,p.product_family
        ,p.product_sub_family
    from cal_cross_sell_revenue c
    left join {{ ref('stg_products_table') }} p
    on p.product_id=c.product_id
)
select 
    * from join_products_table
order by customer_id,product_id, month_year
-- select
--     *
--     ,case 
--         when first_order_date_for_product > first_order_date_for_customer 
--         then total_revenue
--         else 0
--     end as cross_sell_revenue
-- from union_tables
-- order by customer_id,product_id, month_year