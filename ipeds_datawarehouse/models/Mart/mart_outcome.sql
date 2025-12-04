WITH gr AS (
    SELECT * FROM {{ ref('fact_graduates_total') }}
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

    gr.graduation_type,
    gr.total_graduates,
    gr.total_male_graduates,
    gr.total_female_graduates

    
FROM gr 
LEFT JOIN y ON (gr.year = y.academic_year_id)
LEFT JOIN i ON (gr.unit_id = i.unit_id)
