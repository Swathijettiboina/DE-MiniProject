{{config(materialized='table',
    tags=['staging','trans_stg'])}}
with trans_tb as (
    SELECT 
        * 
    from {{source('event_source', 'TRANSACTIONS_TABLE')}}
    where coalesce(CUSTOMER_ID ,
	PRODUCT_ID ,
	PAYMENT_MONTH ,
	REVENUE_TYPE ,
	REVENUE ,
	QUANTITY ,
	DIMENSION_1 ,
	DIMENSION_2 ,
	DIMENSION_3 ,
	DIMENSION_4,
	DIMENSION_5 ,
	DIMENSION_6 ,
	DIMENSION_7,
	DIMENSION_8 ,
	DIMENSION_9 ,
	DIMENSION_10 ,
	COMPANIES ) is NOT NULL
),
cast_columns as(
    SELECT
    {{cast_col_type("CUSTOMER_ID","int")}} as CUSTOMER_ID
    ,{{cast_col_type("PRODUCT_ID","varchar")}} as PRODUCT_ID
    ,TO_DATE(PAYMENT_MONTH ,'DD-MM-YYYY')AS PAYMENT_MONTH
    ,{{cast_col_type("REVENUE_TYPE","int")}} as REVENUE_TYPE
    ,{{cast_col_type("REVENUE","number(20,4)")}} as REVENUE
    ,{{cast_col_type("QUANTITY","int")}} as QUANTITY
    ,{{cast_col_type("COMPANIES","varchar")}} as COMPANIES
    ,{{cast_col_type("DIMENSION_1","varchar")}} as DIMENSION_1
    ,{{cast_col_type("DIMENSION_2","varchar")}} as DIMENSION_2
    ,{{cast_col_type("DIMENSION_3","varchar")}} as DIMENSION_3
    ,{{cast_col_type("DIMENSION_4","varchar")}} as DIMENSION_4
    ,{{cast_col_type("DIMENSION_5","varchar")}} as DIMENSION_5
    ,{{cast_col_type("DIMENSION_6","varchar")}} as DIMENSION_6
    ,{{cast_col_type("DIMENSION_7","varchar")}} as DIMENSION_7
    ,{{cast_col_type("DIMENSION_8","varchar")}} as DIMENSION_8
    ,{{cast_col_type("DIMENSION_9","varchar")}} as DIMENSION_9
    ,{{cast_col_type("DIMENSION_10","varchar")}} as DIMENSION_10
    , ROW_NUMBER() OVER 
        (PARTITION BY CUSTOMER_ID,PRODUCT_ID,PAYMENT_MONTH,REVENUE_TYPE,REVENUE,QUANTITY, COMPANIES 
        ORDER BY  CUSTOMER_ID,PRODUCT_ID,PAYMENT_MONTH,REVENUE_TYPE,REVENUE,QUANTITY, COMPANIES ) as row_num
    from trans_tb
),
remove_duplicates as(
    select 
    *
    from cast_columns
)
SELECT
    * from cal_revenue