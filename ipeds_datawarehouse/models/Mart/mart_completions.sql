WITH c AS (
    SELECT * FROM {{ ref('fact_completions_total') }}
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

    c.total_completions,
    c.total_male_completions,
    c.total_female_completions

    
FROM c 
LEFT JOIN y ON (c.year = y.academic_year_id)
LEFT JOIN i ON (c.unit_id = i.unit_id)