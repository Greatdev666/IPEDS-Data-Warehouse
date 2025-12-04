WITH a AS (
    SELECT * FROM {{ ref('fact_admissions') }}
),
y AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
),
i AS (
    SELECT * FROM {{ ref('dim_institutions') }}
)

SELECT 
    a.academic_year_id,
    y.year_start_date, 

    i.unit_id,
    i.institution_name,
    i.city,
    i.state_abbreviation,
    i.control_type,
    i.sector,
    i.institution_level,

    a.total_applicants,
    a.total_admissions, 
    a.total_enrolled
FROM a 
LEFT JOIN y ON (a.academic_year_id = y.academic_year_id)
LEFT JOIN i ON (a.unit_id = i.unit_id)