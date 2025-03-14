{{config (materialized='table', tags=['intermediate','new_customers'])}}

with first_last_order_dates as (
    select
        customer_id
        ,min(payment_month) as first_order_date
        ,max(payment_month) as last_order_date
    from {{ ref('stg_transactions_table') }}
    group by customer_id
),
min_order_date as (
    select
        min(payment_month) as min_order_date
        from {{ ref('stg_transactions_table') }}
),
new_customers as (
    select
        customer_id
        , first_order_date
        , last_order_date
    from  first_last_order_dates 
    where first_order_date >= dateadd(month, -3, (select min_order_date from min_order_date))
)
select
    * 
    from new_customers