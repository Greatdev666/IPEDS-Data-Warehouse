WITH f2008_f3 AS (
    SELECT * FROM {{ source('bronze', 'f2008_f3') }} 
)

, f2008_f3_renamed AS (

    SELECT
        CAST(2008 AS INT) AS "YEAR",
        -- =====================
        -- Identifiers
        -- =====================
        CAST("UNITID" AS NUMERIC) AS "UNITID",

        -- =====================
        -- Assets & Liabilities
        -- =====================
        CAST("F3A01" AS NUMERIC) AS "F3A01",       -- Total assets
        CAST(NULL AS NUMERIC) AS "F3A01A",     -- Long-term investments
        CAST(NULL AS NUMERIC) AS "F3A01B",     -- PPE net of accumulated depreciation
        CAST(NULL AS NUMERIC) AS "F3A01C",     -- Intangible assets, net
        CAST("F3A02" AS NUMERIC) AS "F3A02",       -- Total liabilities
        CAST(NULL AS NUMERIC) AS "F3A02A",     -- Debt related to PPE
        CAST("F3A03" AS NUMERIC) AS "F3A03",       -- Total equity
        CAST("F3A04" AS NUMERIC) AS "F3A04",       -- Total liabilities and equity
        CAST(NULL AS NUMERIC) AS "F3A05",       -- Land and land improvements
        CAST(NULL AS NUMERIC) AS "F3A06",       -- Buildings
        CAST(NULL AS NUMERIC) AS "F3A07",       -- Equipment including art/library
        CAST(NULL AS NUMERIC) AS "F3A08",       -- Construction in progress
        CAST(NULL AS NUMERIC) AS "F3A10",       -- Total Plant, Property, and Equipment
        CAST(NULL AS NUMERIC) AS "F3A11",       -- Accumulated depreciation
        CAST(NULL AS NUMERIC) AS "F3A12",       -- PPE net (from F3A01B)

        -- =====================
        -- Revenues
        -- =====================
        CAST("F3B01" AS NUMERIC) AS "F3B01",       -- Total revenues and investment return
        CAST("F3D01" AS NUMERIC) AS "F3D01",       -- Tuition and fees
        CAST("F3D02" AS NUMERIC) AS "F3D02",       -- Federal appropriations, grants, contracts
        CAST("F3D03" AS NUMERIC) AS "F3D03",       -- State and local appropriations, grants, contracts
        CAST("F3D04" AS NUMERIC) AS "F3D04",       -- Private gifts, grants, contracts
        CAST("F3D05" AS NUMERIC) AS "F3D05",       -- Investment income and gains/losses
        CAST("F3D06" AS NUMERIC) AS "F3D06",       -- Sales & services of educational activities
        CAST("F3D07" AS NUMERIC) AS "F3D07",       -- Sales & services of auxiliary enterprises
        CAST("F3D08" AS NUMERIC) AS "F3D08",       -- Other revenue
        CAST("F3D09" AS NUMERIC) AS "F3D09",       -- Total revenues and investment return
        CAST(NULL AS NUMERIC) AS "F3D12",       -- Hospital revenue

        -- =====================
        -- Expenses
        -- =====================
        CAST("F3B02" AS NUMERIC) AS "F3B02",       -- Total expenses
        CAST(NULL AS NUMERIC) AS "F3E011",     -- Instruction - Total
        CAST(NULL AS NUMERIC) AS "F3E012",     -- Instruction - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E02A1",   -- Research - Total
        CAST(NULL AS NUMERIC) AS "F3E02A2",   -- Research - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E02B1",   -- Public service - Total
        CAST(NULL AS NUMERIC) AS "F3E02B2",   -- Public service - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E03A1",   -- Academic support - Total
        CAST(NULL AS NUMERIC) AS "F3E03A2",   -- Academic support - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E03B1",   -- Student services - Total
        CAST(NULL AS NUMERIC) AS "F3E03B2",   -- Student services - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E03C1",   -- Institutional support - Total
        CAST(NULL AS NUMERIC) AS "F3E03C2",   -- Institutional support - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E041",     -- Auxiliary enterprises - Total
        CAST(NULL AS NUMERIC) AS "F3E042",     -- Auxiliary enterprises - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E051",     -- Net grant aid to students - Total
        CAST(NULL AS NUMERIC) AS "F3E101",     -- Hospital services - Total
        CAST(NULL AS NUMERIC) AS "F3E102",     -- Hospital services - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E061",     -- Other expenses - Total
        CAST(NULL AS NUMERIC) AS "F3E062",     -- Other expenses - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E071",     -- Total expenses - Total amount
        CAST(NULL AS NUMERIC) AS "F3E072",     -- Total expenses - Salaries & wages
        CAST(NULL AS NUMERIC) AS "F3E073",     -- Total expenses - Benefits
        CAST(NULL AS NUMERIC) AS "F3E074",     -- Total expenses - Operations & maintenance
        CAST(NULL AS NUMERIC) AS "F3E075",     -- Total expenses - Depreciation
        CAST(NULL AS NUMERIC) AS "F3E076",     -- Total expenses - Interest
        CAST(NULL AS NUMERIC) AS "F3E077",     -- Total expenses - All other

        -- =====================
        -- Taxes / Profit
        -- =====================
        CAST(NULL AS NUMERIC) AS "F3F01",       -- Federal income tax expenses
        CAST(NULL AS NUMERIC) AS "F3F02",       -- State & local income tax expenses
        CAST(NULL AS NUMERIC) AS "F3G01",       -- Pretax income
        CAST(NULL AS NUMERIC) AS "F3G02",       -- Total revenues
        CAST(NULL AS NUMERIC) AS "F3G03",       -- Total equity
        CAST(NULL AS NUMERIC) AS "F3G04",       -- Total assets
        CAST(NULL AS NUMERIC) AS "F3G05",       -- Adjusted equity
        CAST(NULL AS NUMERIC) AS "F3G06",       -- Plant-related debt
        CAST(NULL AS NUMERIC) AS "F3G07"        -- Total expenses (alternate)

    FROM f2008_f3

)

SELECT *
FROM f2008_f3_renamed 
