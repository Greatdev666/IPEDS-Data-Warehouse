WITH 
awl AS (
    SELECT * FROM {{ ref('dim_award_level') }}
),
cal AS (
    SELECT 
        "UNITID" as unit_id,
        "YEAR" as year,
        "AWLEVEL" as award_level,
        SUM("CTOTALT") as total_completions
    FROM {{ ref('cyyyy_a') }}
    GROUP BY "UNITID", "AWLEVEL", "YEAR"
)

SELECT  
    cal.year,
    cal.unit_id,
    awl.award_level_code,
    COALESCE(cal.total_completions, 0) AS total_completions
FROM cal 
LEFT JOIN awl ON (awl.award_level_code = cal.award_level)
