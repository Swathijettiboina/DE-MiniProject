{{config (materialized='table', tags=['intermediate','churn'])}}

with first_last_order_dates as (
    select
        customer_id
        ,min(payment_month) as first_order_date
        ,max(payment_month) as last_order_date
    from {{ ref('stg_transactions_table') }}
    group by customer_id
),
max_order_date as (
    select
        max(payment_month) as max_order_date
        from {{ ref('stg_transactions_table') }}
),
churned_customers as (
    select
        customer_id
        , first_order_date
        , last_order_date
    from  first_last_order_dates 
    where last_order_date <= dateadd(month, -3, (select max_order_date from max_order_date))
)
select
    * 
    from churned_customers