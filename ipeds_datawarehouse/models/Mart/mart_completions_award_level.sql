WITH c AS (
    SELECT * FROM {{ ref('fact_completion_award_level') }}
),
y AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
),
i AS (
    SELECT * FROM {{ ref('dim_institutions') }}
),
al AS (
    SELECT * FROM {{ ref('dim_award_level') }}
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

    SUM(c.total_completions) AS total_completions,
    -- Level-based enrollment
    SUM(CASE WHEN c.award_level_code = 3 THEN c.total_completions END) AS asssociates_degree,
    SUM(CASE WHEN c.award_level_code = 5 THEN c.total_completions END) AS bachelors_degree,
    SUM(CASE WHEN c.award_level_code = 7 THEN c.total_completions END) AS masters_degree,
    SUM(CASE WHEN c.award_level_code = 17 THEN c.total_completions END) AS research_scholarship_doctors_degree,
    SUM(CASE WHEN c.award_level_code = 18 THEN c.total_completions END) AS professional_practice_doctors_degree,
    SUM(CASE WHEN c.award_level_code = 19 THEN c.total_completions END) AS other_doctor_degree

FROM c 
LEFT JOIN y ON (c.year = y.academic_year_id)
LEFT JOIN i ON (c.unit_id = i.unit_id)
LEFT JOIN al ON (c.award_level_code = al.award_level_code)
GROUP BY y.academic_year_id, y.year_start_date, y.year_end_date,
i.unit_id, 
i.institution_name, i.city, 
i.state_abbreviation, i.control_type, i.sector, i.institution_level

