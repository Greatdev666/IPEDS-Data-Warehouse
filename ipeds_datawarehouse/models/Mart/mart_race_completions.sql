WITH 
c AS (
        SELECT * FROM {{ ref('fact_completion_by_race') }}
),
y AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
),
i AS (
    SELECT * FROM {{ ref('dim_institutions') }}
),
r AS (
    SELECT * FROM {{ ref('dim_race_ethnicity') }}
)

SELECT
    y.academic_year_id,
    y.year_start_date,
    y.year_end_date,

    i.unit_id,
    i.institution_name,
    i.city,
    i.state_abbreviation,
    i.control_type,
    i.sector,
    i.institution_level,

    SUM(c.total) AS total_completions,
    -- Pivot the 9 race categories
    SUM(CASE WHEN r.race_ethnicity_sk = 1 THEN c.total END) AS white_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 2 THEN c.total END) AS black_or_african_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 3 THEN c.total END) AS hispanic_or_latino_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 4 THEN c.total END) AS asian_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 5 THEN c.total END) AS american_indian_or_alaska_native_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 6 THEN c.total END) AS pacific_islander_or_native_hawaiian_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 7 THEN c.total END) AS two_or_more_races_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 8 THEN c.total END) AS nonresident_alien_completions,
    SUM(CASE WHEN r.race_ethnicity_sk = 9 THEN c.total END) AS unknown_race_completions


FROM c  
LEFT JOIN r ON c.race_ethnicity_sk = r.race_ethnicity_sk
LEFT JOIN y ON c.year = y.academic_year_id
LEFT JOIN i ON c.unit_id = i.unit_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
