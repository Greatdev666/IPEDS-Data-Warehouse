WITH 
enr AS (
    SELECT 
        "YEAR" as year,
        "UNITID" as unit_id,
        "EFFYLEV" as enrollment_level,
        SUM("EFYTOTLT") as total_enrolled,
        SUM("EFYTOTLM") as total_male_enrolled,
        SUM("EFYTOTLW") as total_female_enrolled
     FROM {{ ref('effyyyyy') }}
     GROUP BY 1,2,3
) 

SELECT 
    year,
    unit_id,
    enrollment_level,
    COALESCE(total_enrolled, 0) AS total_enrolled,
    COALESCE(total_male_enrolled, 0) AS total_male_enrolled,
    COALESCE(total_female_enrolled, 0) AS total_female_enrolled
FROM enr
 