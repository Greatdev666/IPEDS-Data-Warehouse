WITH 
e AS (
        SELECT * FROM {{ ref('fact_enrollment_fullyear_race') }}
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

    SUM(e.total) AS total_enrollment,
    -- Pivot the 9 race categories
    SUM(CASE WHEN r.race_ethnicity_sk = 1 THEN e.total END) AS white_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 2 THEN e.total END) AS black_or_african_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 3 THEN e.total END) AS hispanic_or_latino_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 4 THEN e.total END) AS asian_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 5 THEN e.total END) AS american_indian_or_alaska_native_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 6 THEN e.total END) AS pacific_islander_or_native_hawaiian_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 7 THEN e.total END) AS two_or_more_races_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 8 THEN e.total END) AS nonresident_alien_enrollment,
    SUM(CASE WHEN r.race_ethnicity_sk = 9 THEN e.total END) AS unknown_race_enrollment


FROM e
LEFT JOIN r ON e.race_ethnicity_sk = r.race_ethnicity_sk
LEFT JOIN y ON e.year = y.academic_year_id
LEFT JOIN i ON e.unit_id = i.unit_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
