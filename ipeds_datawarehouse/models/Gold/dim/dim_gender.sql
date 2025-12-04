{{
  config(
    materialized = 'table',
    )
}}

WITH gender AS (
    SELECT 1 AS gender_sk, 'Male' AS  gender 
    UNION ALL 
    SELECT 2, 'Female'
    UNION ALL 
    SELECT 3, 'Another Gender'
    UNION ALL
    SELECT 4, 'Gender Unknown'
)

SELECT * FROM gender