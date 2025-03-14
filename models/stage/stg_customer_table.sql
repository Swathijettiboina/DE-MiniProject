{{config(materialized='table',
    tags=['staging'])}}
with customer_tb as (
    SELECT 
        * 
    from {{source('event_source', 'CUSTOMER_TABLE')}}
    where coalesce(COMPANY,CUSTOMERNAME,CUSTOMER_ID) is NOT NULL
),
cast_columns as(
    SELECT
    {{ cast_col_type('customer_id',"int")}} as customer_id
    ,{{cast_col_type('customername',"varchar")}} as customer_name 
    ,{{cast_col_type('company',"varchar")}} as company
    , ROW_NUMBER() OVER (PARTITION BY customer_id,company,customername ORDER BY customer_id,company,customername) as row_num
    from customer_tb
),
remove_duplicates as(
    select 
        customer_id
        ,customer_name
        ,company
    from cast_columns
    where row_num=1
)
SELECT
    * from remove_duplicates