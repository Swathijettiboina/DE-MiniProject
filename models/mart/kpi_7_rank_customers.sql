{{config(materialized='table', tags=['mart','KPI_7_rank_customers','kpi_7'])}}

select * from {{ ref('int_rank_customers') }}