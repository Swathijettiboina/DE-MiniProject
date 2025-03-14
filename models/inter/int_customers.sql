{{config (materialized='table', tags=['intermediate','new_customers','customers','churn'])}}    

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
max_order_date as (
    select
        max(payment_month) as max_order_date
        from {{ ref('stg_transactions_table') }}
),
customer_analysis  as(
    select
        customer_id
        , first_order_date
        , last_order_date
        , case 
            when last_order_date <= dateadd(month, -3, (select max_order_date from max_order_date)) then 'Churned Customer'
            when first_order_date >= dateadd(month, 3, (select min_order_date from min_order_date)) then 'New Customer'
            else 'active'
        end as customer_status
    from  first_last_order_dates
)
select
    * 
    from customer_analysis