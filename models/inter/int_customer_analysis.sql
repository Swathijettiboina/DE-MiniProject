{{
    config(materialized='table',
    tags=['intermediate','anlysis'])
}}

select * from {{customer_analisys(2)}}