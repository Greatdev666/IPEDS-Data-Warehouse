{{ config(materialized='table') }}

WITH base_f1 AS (

    SELECT DISTINCT 
        "YEAR" AS year,
        "UNITID" AS unit_id, 
        'F1' AS finance_type,

        -- ===== Balance Sheet =====
        "F1A06"  AS total_assets,
        "F1A01"  AS total_current_assets,
        "F1A05"  AS total_noncurrent_assets,
        "F1A27T4" AS ppe_total,
        "F1A284" AS ppe_accumulated_depreciation,
        "F1A274" AS ppe_construction_in_progress,
        "F1A13"  AS total_liabilities,
        "F1A09"  AS total_current_liabilities,
        "F1A12"  AS total_noncurrent_liabilities,
        "F1A18"  AS net_assets_or_position,

        -- Endowment
        "F1H01" AS endowment_begin,
        "F1H02" AS endowment_end,
        "F1H03" AS endowment_change,

        -- ===== Revenue =====
        "F1B01"  AS tuition_fees,
        "F1B02"  AS federal_revenue,
        "F1B03"  AS state_revenue,
        "F1B04B" AS private_revenue,
        "F1B05"  AS auxiliary_enterprises_revenue,
        "F1B09"  AS total_operating_revenue,
        "F1B10"  AS total_nonoperating_revenue,
        "F1B27"  AS total_revenue,
        "F1B17"  AS investment_income,
        "F1B13"  AS other_revenue,
        "F1B26"  AS sales_educational_services,
        "F1B06"  AS hospital_revenue, 

        -- ===== Expenses =====
        "F1C011" AS expense_instruction,
        "F1C021" AS expense_research,
        "F1C031" AS expense_public_service,
        "F1C051" AS expense_academic_support,
        "F1C061" AS expense_student_services,
        "F1C071" AS expense_institutional_support,
        "F1C111" AS expense_auxiliary_enterprises,
        "F1C101" AS expense_scholarships,
        "F1C191" AS total_expenses

    FROM {{ ref('fyyyy_f1') }}

),

base_f2 AS (

    SELECT DISTINCT 
        "YEAR" AS year,
        "UNITID" AS unit_id,
        'F2' AS finance_type,

        "F2A02" AS total_assets,
        CAST(NULL AS NUMERIC) AS total_current_assets,
        CAST(NULL AS NUMERIC) AS total_noncurrent_assets,
        "F2A17" AS ppe_total,
        "F2A18" AS ppe_accumulated_depreciation,
        "F2A15" AS ppe_construction_in_progress,
        "F2A03" AS total_liabilities, 
        CAST(NULL AS NUMERIC) AS total_current_liabilities,
        CAST(NULL AS NUMERIC) AS total_noncurrent_liabilities,
        "F2A06" AS net_assets_or_position,

        -- Endowment
        "F2H01" AS endowment_begin,
        "F2H02" AS endowment_end,
        "F2H03" AS endowment_change,

        -- Revenue
        "F2D01" AS tuition_fees,
        "F2D05" AS federal_revenue,
        "F2D06" AS state_revenue,
        "F2D08" AS private_revenue,
        "F2D12" AS auxiliary_enterprises_revenue,
        "F2D11" AS sales_educational_services,
        "F2D10" AS investment_income,
        "F2D13" AS hospital_revenue,
        "F2D15" AS other_revenue,
        "F2D16" AS total_operating_revenue,
        "F2D17" AS total_nonoperating_revenue,
        "F2D18" AS total_revenue,

        -- Expenses
        "F2E011" AS expense_instruction,
        "F2E021" AS expense_research,
        "F2E031" AS expense_public_service,
        "F2E041" AS expense_academic_support,
        "F2E051" AS expense_student_services,
        "F2E061" AS expense_institutional_support,
        "F2E071" AS expense_auxiliary_enterprises,
        "F2E081" AS expense_scholarships,
        "F2E131" AS total_expenses

    FROM {{ ref('fyyyy_f2') }}

),

base_f3 AS (

    SELECT DISTINCT
        "YEAR" AS year,
        "UNITID" AS unit_id,
        'F3' AS finance_type,

        "F3A01" AS total_assets,
        CAST(NULL AS NUMERIC) AS total_current_assets,
        CAST(NULL AS NUMERIC) AS total_noncurrent_assets,
        "F3A10" AS ppe_total,
        "F3A11" AS ppe_accumulated_depreciation,
        "F3A08" AS ppe_construction_in_progress,
        "F3A02" AS total_liabilities,
        CAST(NULL AS NUMERIC) AS total_current_liabilities,
        CAST(NULL AS NUMERIC) AS total_noncurrent_liabilities,
        "F3A03" AS net_assets_or_position,

        CAST(NULL AS NUMERIC) AS endowment_begin,
        CAST(NULL AS NUMERIC) AS endowment_end,
        CAST(NULL AS NUMERIC) AS endowment_change,

        "F3D01" AS tuition_fees,
        "F3D02" AS federal_revenue,
        "F3D03" AS state_revenue,
        "F3D04" AS private_revenue,
        "F3D07" AS auxiliary_enterprises_revenue,
        "F3D06" AS sales_educational_services,
        "F3D05" AS investment_income,
        "F3D12" AS hospital_revenue,
        "F3D08" AS other_revenue,
        "F3D09" AS total_operating_revenue,
        CAST(NULL AS NUMERIC) AS total_nonoperating_revenue,
        "F3D09" AS total_revenue,

        "F3E011" AS expense_instruction,
        "F3E02A1" AS expense_research,
        "F3E02B1" AS expense_public_service,
        "F3E03A1" AS expense_academic_support,
        "F3E03B1" AS expense_student_services,
        "F3E03C1" AS expense_institutional_support,
        "F3E041" AS expense_auxiliary_enterprises,
        "F3E051" AS expense_scholarships,
        "F3B02"  AS total_expenses

    FROM {{ ref('fyyyy_f3') }}

),

unioned AS (
    SELECT * FROM base_f1
    UNION ALL
    SELECT * FROM base_f2
    UNION ALL
    SELECT * FROM base_f3
),

unpivoted AS (

    SELECT
        year,
        CAST(unit_id AS INTEGER) AS unit_id,
        finance_type,
        c.finance_category_sk,

        CASE c.category_code
            WHEN 'TOTAL_ASSETS' THEN total_assets
            WHEN 'TOTAL_CURRENT_ASSETS' THEN total_current_assets
            WHEN 'TOTAL_NONCURRENT_ASSETS' THEN total_noncurrent_assets
            WHEN 'LONG_TERM_INVESTMENTS' THEN NULL

            WHEN 'TOTAL_LIABILITIES' THEN total_liabilities
            WHEN 'TOTAL_CURRENT_LIABILITIES' THEN total_current_liabilities
            WHEN 'TOTAL_NONCURRENT_LIABILITIES' THEN total_noncurrent_liabilities
            WHEN 'NET_ASSETS_OR_POSITION' THEN net_assets_or_position

            WHEN 'ENDOWMENT_BEGIN' THEN endowment_begin
            WHEN 'ENDOWMENT_END' THEN endowment_end
            WHEN 'ENDOWMENT_CHANGE' THEN endowment_change

            WHEN 'PPE_TOTAL' THEN ppe_total
            WHEN 'PPE_ACCUMULATED_DEPRECIATION' THEN ppe_accumulated_depreciation
            WHEN 'PPE_CONSTRUCTION_IN_PROGRESS' THEN ppe_construction_in_progress

            WHEN 'TUITION_FEES' THEN tuition_fees
            WHEN 'FEDERAL_REVENUE' THEN federal_revenue
            WHEN 'STATE_REVENUE' THEN state_revenue
            WHEN 'PRIVATE_REVENUE' THEN private_revenue
            WHEN 'AUXILIARY_ENTERPRISES_REVENUE' THEN auxiliary_enterprises_revenue
            WHEN 'SALES_EDUCATIONAL_SERVICES' THEN sales_educational_services
            WHEN 'INVESTMENT_INCOME' THEN investment_income
            WHEN 'HOSPITAL_REVENUE' THEN hospital_revenue
            WHEN 'OTHER_REVENUE' THEN other_revenue
            WHEN 'TOTAL_OPERATING_REVENUE' THEN total_operating_revenue
            WHEN 'TOTAL_NONOPERATING_REVENUE' THEN total_nonoperating_revenue
            WHEN 'TOTAL_REVENUE' THEN total_revenue

            WHEN 'EXPENSE_INSTRUCTION' THEN expense_instruction
            WHEN 'EXPENSE_RESEARCH' THEN expense_research
            WHEN 'EXPENSE_PUBLIC_SERVICE' THEN expense_public_service
            WHEN 'EXPENSE_ACADEMIC_SUPPORT' THEN expense_academic_support
            WHEN 'EXPENSE_STUDENT_SERVICES' THEN expense_student_services
            WHEN 'EXPENSE_INSTITUTIONAL_SUPPORT' THEN expense_institutional_support
            WHEN 'EXPENSE_AUXILIARY_ENTERPRISES' THEN expense_auxiliary_enterprises
            WHEN 'EXPENSE_SCHOLARSHIPS' THEN expense_scholarships
            WHEN 'TOTAL_EXPENSES' THEN total_expenses
        END AS amount

    FROM unioned u
    CROSS JOIN {{ ref('dim_finance_category') }} c
)
 
SELECT
    year,
    unit_id,
    finance_type,
    finance_category_sk,
    COALESCE(amount, 0) AS amount
FROM unpivoted
