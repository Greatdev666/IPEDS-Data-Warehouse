WITH 
com AS (
    SELECT  
        "YEAR" as year,
        "UNITID" as unit_id,
        SUM("CSTOTLT") as total_completions,
        SUM("CSTOTLM") as total_male_completions,
        SUM("CSTOTLW") as total_female_completions
     FROM {{ ref('cyyyy_b') }}
     GROUP BY 1,2
)

SELECT 
    year,
    unit_id,
    COALESCE(total_completions, 0) AS total_completions,
    COALESCE(total_male_completions, 0) AS total_male_completions,
    COALESCE(total_female_completions, 0) AS total_female_completions
FROM com
