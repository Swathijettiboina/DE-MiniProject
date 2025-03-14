 {{ config(
    materialized='table'
    , tags=['intermediate','fiscal_year_new'])
}}

WITH customer_entered AS (
    SELECT
        customer_id
        , MIN(EXTRACT(YEAR FROM payment_month)) AS customer_start_year
    FROM
        {{ ref('stg_transactions_table') }}
    GROUP BY
        customer_id
)
SELECT
    customer_start_year
    , COUNT(DISTINCT customer_id)  AS new_customers_count
FROM
    customer_entered
group by 1