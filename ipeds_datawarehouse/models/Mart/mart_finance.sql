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
),
e AS (
    SELECT * FROM {{ ref('fact_enrollment_fullyear_total') }}
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

    e.total_enrolled,
     -- ===== Finance Pulls (Pivoting by aggregation) =====
    SUM(CASE WHEN fc.category_code = 'TOTAL_REVENUE'
             THEN f.amount END) AS total_revenue,

    SUM(CASE WHEN fc.category_code = 'TOTAL_EXPENSES'
             THEN f.amount END) AS total_expenses,

    SUM(CASE WHEN fc.category_code = 'ENDOWMENT_END'
             THEN f.amount END) AS endowment_value,

    SUM(CASE WHEN fc.category_code = 'NET_ASSETS_OR_POSITION'
             THEN f.amount END) AS net_position,

    -- ===== Revenue Composition =====
    SUM(CASE WHEN fc.category_code = 'TUITION_FEES'
             THEN f.amount END) AS tuition_revenue,
    SUM(CASE WHEN fc.category_code = 'FEDERAL_REVENUE'
             THEN f.amount END) AS federal_rev,
    SUM(CASE WHEN fc.category_code = 'STATE_REVENUE'
             THEN f.amount END) AS state_rev,
    SUM(CASE WHEN fc.category_code = 'PRIVATE_REVENUE'
             THEN f.amount END) AS private_rev
    
FROM f 
LEFT JOIN fc ON (f.finance_category_sk = fc.finance_category_sk)
LEFT JOIN y ON (f.year = y.academic_year_id)
LEFT JOIN i ON (f.unit_id = i.unit_id)
LEFT JOIN e ON (f.unit_id = e.unit_id)
AND f.year = e.year
GROUP BY 
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
e.total_enrolled