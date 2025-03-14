{{config (materialized='table', tags=['mart','KPI_1_churn_count'])}}


with churned_count as(
    select
       count(customer_id) as churned_customers_count
    from {{ ref('int_customers') }} c
    where c.customer_status = 'Churned Customer'
),
new_customers_count as(
    select
       count(customer_id) as new_customers_count
    from {{ ref('int_customers') }} c
    where c.customer_status = 'New Customer'
),
active_customers_count as(
    select
       count(customer_id) as active_customers_count
    from {{ ref('int_customers') }} c
    where c.customer_status = 'active'
)
select
    churned_customers_count
    ,new_customers_count
    ,active_customers_count
from churned_count, new_customers_count, active_customers_count
