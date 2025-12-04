{{ config(materialized='table') }}

WITH categories AS (
    SELECT 1 AS race_ethnicity_sk, 'White' AS category_name
    UNION ALL
    SELECT 2, 'Black or African American'
    UNION ALL
    SELECT 3, 'Hispanic or Latino'
    UNION ALL
    SELECT 4, 'Asian'
    UNION ALL
    SELECT 5, 'American Indian or Alaska Native'
    UNION ALL
    SELECT 6, 'Native Hawaiian or Pacific Islander'
    UNION ALL
    SELECT 7, 'Two or More Races'
    UNION ALL
    SELECT 8, 'US Nonresident Alien'
    UNION ALL
    SELECT 9, 'Race/ethnicity unknown'
)

SELECT * 
FROM categories
 