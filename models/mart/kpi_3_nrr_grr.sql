{{
    config(materialized='table',
    tags=['mart','kpi_3'])
}}
select 
    * 
from {{ref("int_metrics")}}