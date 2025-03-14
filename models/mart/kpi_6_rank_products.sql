{{config(materialized='table', tags=['mart','KPI_6_rank_products'])}}

select * from {{ ref('int_rank_products') }}