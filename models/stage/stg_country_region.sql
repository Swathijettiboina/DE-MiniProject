{{config(materialized='table',
    tags=['staging'])}}
with region_tb as (
    SELECT 
        * 
    from {{source('event_source', 'COUNTRY_REGION_TABLE')}}
    where coalesce(CUSTOMER_ID,COUNTRY,REGION) is NOT NULL
),
cast_columns as(
    SELECT
    {{cast_col_type("CUSTOMER_ID","int")}} as CUSTOMER_ID
    ,{{cast_col_type("COUNTRY","varchar")}} as COUNTRY
    ,{{cast_col_type("REGION","varchar")}} as REGION 
    , ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID,COUNTRY,REGION ORDER BY CUSTOMER_ID,COUNTRY,REGION) as row_num
    from region_tb
),
remove_duplicates as(
    select 
       CUSTOMER_ID,COUNTRY,REGION
    from cast_columns
    where row_num=1
)
SELECT
    * from remove_duplicates