{{ config(materialized='table') }}

WITH age_category AS (

    SELECT 1 AS age_category_sk, 1 AS ef_age_code, 'Under 18' AS age_label
    UNION ALL SELECT 2, 2, '18-19'
    UNION ALL SELECT 3, 3, '20-21'
    UNION ALL SELECT 4, 4, '22-24'
    UNION ALL SELECT 5, 5, '25-29'
    UNION ALL SELECT 6, 6, '30-34'
    UNION ALL SELECT 7, 7, '35-39'
    UNION ALL SELECT 8, 8, '40-49'
    UNION ALL SELECT 9, 9, '50-64'
    UNION ALL SELECT 10, 10, '65 and over'
    UNION ALL SELECT 11, 99, 'Age unknown'

)

SELECT * FROM age_category
