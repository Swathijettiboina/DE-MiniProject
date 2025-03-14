{{
    config(materialized='table',
    tags=['mart','kpi_5'])
}}
select * from {{ ref('int_fiscal_year_new') }}