{{config(materialized='table',
    tags=['staging'])}}
with product_tb as (
    SELECT 
        * 
    from {{source('event_source', 'PRODUCTS_TABLE')}}
    where coalesce(PRODUCT_ID,PRODUCT_SUB_FAMILY,PRODUCT_FAMILY) is NOT NULL
),
cast_columns as(
    SELECT
    {{cast_col_type("PRODUCT_ID","varchar")}} as PRODUCT_ID
    ,{{cast_col_type("PRODUCT_SUB_FAMILY","varchar")}} as PRODUCT_SUB_FAMILY
    ,{{cast_col_type("PRODUCT_FAMILY","varchar")}} as PRODUCT_FAMILY 
    , ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID,PRODUCT_SUB_FAMILY,PRODUCT_FAMILY ORDER BY PRODUCT_ID,PRODUCT_SUB_FAMILY,PRODUCT_FAMILY) as row_num
    from product_tb
),
remove_duplicates as(
    select 
       PRODUCT_ID,PRODUCT_SUB_FAMILY,PRODUCT_FAMILY
    from cast_columns
    where row_num=1
)
SELECT
    * from remove_duplicates