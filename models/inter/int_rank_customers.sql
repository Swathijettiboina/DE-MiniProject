{{config(materialized='table',
    tags=['intermediate','rank_customers'])}}
with customer_revenue as (
    select
        customer_id
        ,sum(quantity*revenue) as total_revenue
    from {{ ref('stg_transactions_table') }}
    group by customer_id
),
ranked_customers as (
    select
        customer_id
        ,total_revenue
        ,rank() over (order by total_revenue desc) as customer_rank
    from customer_revenue
)
select
    *
    from ranked_customers