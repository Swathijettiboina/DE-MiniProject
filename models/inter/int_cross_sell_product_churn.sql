{{
    config(materialized='table',
    tags=['intermediate','cross_sell_product_churn',"cross"])
}}
-- with join_products_transactions as (
--     select
--         t.customer_id
--         ,t.product_id
--         ,t.payment_month
--         -- ,FORMAT(t.payment_month, 'MM/yyyy') AS payment_month
--         ,t.revenue_type
--         ,t.revenue
--         ,t.quantity
--         ,p.product_family
--         ,p.product_sub_family
--         ,t.companies
--     from {{ ref('stg_transactions_table') }} t
--     join {{ ref('stg_products_table') }} p
--     on t.product_id = p.product_id
-- ),

with transaction_table as(
    select
        t.customer_id
        ,t.product_id
        ,t.payment_month
        ,(t.revenue * t.quantity) as total_revenue
    from {{ ref('stg_transactions_table') }} t
),
min_payment_month as(
    select
        customer_id
        ,min(payment_month) as min_payment_month
    from transaction_table
    group by 1
),
join_trans_min as(
    select
        t.customer_id
        ,t.product_id
        ,t.payment_month
        ,t.total_revenue
        ,m.min_payment_month as first_payment_month
    from transaction_table t
    join min_payment_month m
    on t.customer_id = m.customer_id
),
cross_sell_cte as(
    select 
        j.*
        ,case 
            when j.payment_month > j.first_payment_month 
            then 1
            else 0
        end as cross_sell
    from join_trans_min j       
)
select
    *
from cross_sell_cte