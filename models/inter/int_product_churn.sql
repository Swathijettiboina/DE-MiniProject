{{config(materialized='table',
    tags=['intermediate','product_churn'])}}

with transaction_table as(
    select
        t.customer_id
        ,t.product_id
        ,max(payment_month) over(partition by t.customer_id) as last_order_date_customer
        ,min(payment_month) over(partition by t.customer_id) as first_order_date_customer
        ,max(payment_month) over(partition by t.customer_id,t.product_id) as last_order_date_product
        ,min(payment_month) over(partition by t.customer_id,t.product_id) as first_order_date_product
    from {{ ref('stg_transactions_table') }} t
),
min_payment_month_dataset as(
    select min(payment_month) as min_payment_month
    from {{ ref('stg_transactions_table') }}
),
max_payment_month_dataset as(
    select max(payment_month) as max_payment_month
    from {{ ref('stg_transactions_table') }}
),
churn_table as(
    select
        customer_id
        ,product_id
        ,first_order_date_customer
        ,last_order_date_customer
        ,first_order_date_product
        ,last_order_date_product
        ,case 
            when last_order_date_product < last_order_date_customer then 'product_churn'
            else 'product_not_churn'
        end as product_churn
        ,case when last_order_date_customer < (select max_payment_month from max_payment_month_dataset) then 'customer_churn'
        else 'customer_not_churn'
        end as customer_churn
    from transaction_table
)
select 
    *
from churn_table
