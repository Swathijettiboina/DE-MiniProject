{{
    config(materialized='table',
    tags=['mart','kpi_4'])
}}
with transaction_table as(
    select
        concat(EXTRACT(year from payment_month), '-', LPAD(EXTRACT(month from payment_month)::text, 2, '0')) as month_year
        ,sum(revenue*quantity) as total_revenue
    from {{ ref('stg_transactions_table') }}
    group by 1
),
lag_operation as(
    select 
        month_year
        ,total_revenue
        ,LAG(total_revenue) OVER(ORDER BY month_year) as previous_revenue
    from transaction_table
)
select
    *
    ,case WHEN previous_revenue>total_revenue
        then previous_revenue-total_revenue
        else 0
    end as loss_of_revenue
from lag_operation
order by month_year