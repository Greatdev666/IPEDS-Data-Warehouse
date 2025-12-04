{{
  config(
    materialized = 'table',
    )
}}

WITH learning_mode AS (
    SELECT 1 AS learning_mode_key, 'Fully Online' AS  learning_mode 
    UNION ALL 
    SELECT 2, 'Hybrid'
    UNION ALL 
    SELECT 3, 'In-Person'
)

SELECT * FROM learning_mode 