WITH e AS (
    SELECT * FROM {{ ref('fact_enrollment_fullyear_total') }} 
),
y AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
),
i AS (
    SELECT * FROM {{ ref('dim_institutions') }}
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
    
    e.enrollment_level,
    e.total_enrolled,
    e.total_male_enrolled,
    e.total_female_enrolled
FROM e
LEFT JOIN y ON (e.year = y.academic_year_id)
LEFT JOIN i ON (e.unit_id = i.unit_id)