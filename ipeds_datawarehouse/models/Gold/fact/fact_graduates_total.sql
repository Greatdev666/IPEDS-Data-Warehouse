WITH 
gr AS (
    SELECT 
        "YEAR" as year,
        "UNITID" as unit_id,
        "GRTYPE" as graduation_type,
        SUM("GRTOTLT") as total_graduates,
        SUM("GRTOTLM") as total_male_graduates,
        SUM("GRTOTLW") as total_female_graduates
     FROM {{ ref('gryyyy') }}
     GROUP BY 1,2,3
)
 
SELECT 
    year,
    unit_id,
    graduation_type,
    COALESCE(total_graduates, 0) AS total_graduates,
    COALESCE(total_male_graduates, 0) AS total_male_graduates,
    COALESCE(total_female_graduates, 0) AS total_female_graduates
FROM gr
