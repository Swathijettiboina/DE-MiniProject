{{
    config(materialized='table',
    tags=['mart','kpi','cross_cell','kpi_2'])
}}

with cross_sell_cte as(
    select 
        customer_id
        ,count(cross_sell) as cross_sell_count
    from {{ ref('int_cross_sell_product_churn') }}
    group by customer_id
),
rank_customers_cte as(
    select
        customer_id
        ,cross_sell_count
        ,rank() over(order by cross_sell_count desc) as rank_number
    from cross_sell_cte
)
select
    *
from
    rank_customers_cte