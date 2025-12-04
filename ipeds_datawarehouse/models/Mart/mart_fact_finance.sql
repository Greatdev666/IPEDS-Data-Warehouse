WITH f AS (
    SELECT * FROM {{ ref('fact_finance') }}
),
fc AS (
    SELECT * FROM {{ ref('dim_finance_category') }}
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

    f.finance_category_sk,
    fc.category_code,
    f.amount

FROM f 
LEFT JOIN fc ON (f.finance_category_sk = fc.finance_category_sk)
LEFT JOIN y ON (f.year = y.academic_year_id)
LEFT JOIN i ON (f.unit_id = i.unit_id)

