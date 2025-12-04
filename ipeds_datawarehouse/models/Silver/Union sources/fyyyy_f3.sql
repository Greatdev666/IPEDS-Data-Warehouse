WITH fyyyy_f3 AS (
    SELECT * FROM {{ ref('f2023f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2022f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2021f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2020f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2019f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2018f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2017f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2016f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2015f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2014f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2013f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2012f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2011f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2010f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2009f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2008f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2007f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2006f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2005f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2004f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2003f3') }}
    UNION ALL
    SELECT * FROM {{ ref('f2002f3') }}
)

SELECT 
    "YEAR", 
    "UNITID",        -- Unique identification number of the institution

    -- =====================
    -- Assets & Liabilities
    -- =====================
    "F3A01",         -- Total assets
    "F3A01A",        -- Long-term investments
    "F3A01B",        -- Property, plant, and equipment, net of accumulated depreciation
    "F3A01C",        -- Intangible assets, net of accumulated amortization
    "F3A02",         -- Total liabilities
    "F3A02A",        -- Debt related to property, plant, and equipment
    "F3A03",         -- Total equity
    "F3A04",         -- Total liabilities and equity
    "F3A05",         -- Land and land improvements
    "F3A06",         -- Buildings
    "F3A07",         -- Equipment, including art and library collections
    "F3A08",         -- Construction in progress
    "F3A10",         -- Total Plant, Property, and Equipment
    "F3A11",         -- Accumulated depreciation
    "F3A12",         -- PPE net (from F3A01B)
    -- =====================
    -- Revenues
    -- =====================
    "F3B01",         -- Total revenues and investment return
    "F3D01",         -- Tuition and fees
    "F3D02",         -- Federal appropriations, grants, and contracts
    "F3D03",         -- State and local appropriations, grants, and contracts
    "F3D04",         -- Private gifts, grants, and contracts
    "F3D05",         -- Investment income and gains/losses
    "F3D06",         -- Sales and services of educational activities
    "F3D07",         -- Sales and services of auxiliary enterprises
    "F3D08",         -- Other revenue
    "F3D09",         -- Total revenues and investment return (summary)
    "F3D12",         -- Hospital revenue
    -- =====================
    -- Expenses
    -- =====================
    "F3B02",         -- Total expenses
    "F3E011",        -- Instruction - Total
    "F3E012",        -- Instruction - Salaries and wages
    "F3E02A1",       -- Research - Total
    "F3E02A2",       -- Research - Salaries and wages
    "F3E02B1",       -- Public service - Total
    "F3E02B2",       -- Public service - Salaries and wages
    "F3E03A1",       -- Academic support - Total
    "F3E03A2",       -- Academic support - Salaries and wages
    "F3E03B1",       -- Student services - Total
    "F3E03B2",       -- Student services - Salaries and wages
    "F3E03C1",       -- Institutional support - Total
    "F3E03C2",       -- Institutional support - Salaries and wages
    "F3E041",        -- Auxiliary enterprises - Total
    "F3E042",        -- Auxiliary enterprises - Salaries and wages
    "F3E051",        -- Net grant aid to students - Total
    "F3E101",        -- Hospital services - Total
    "F3E102",        -- Hospital services - Salaries and wages
    "F3E061",        -- Other expenses - Total
    "F3E062",        -- Other expenses - Salaries and wages
    "F3E071",        -- Total expenses - Total amount
    "F3E072",        -- Total expenses - Salaries and wages
    "F3E073",        -- Total expenses - Benefits
    "F3E074",        -- Total expenses - Operations and maintenance
    "F3E075",        -- Total expenses - Depreciation
    "F3E076",        -- Total expenses - Interest
    "F3E077",        -- Total expenses - All other

    -- =====================
    -- Taxes / Profit
    -- =====================
    "F3F01",         -- Federal income tax expenses
    "F3F02",         -- State and local income tax expenses
    "F3G01",         -- Pretax income
    "F3G02",         -- Total revenues (alternate)
    "F3G03",         -- Total equity
    "F3G04",         -- Total assets
    "F3G05",         -- Adjusted equity
    "F3G06",         -- Plant-related debt
    "F3G07"         -- Total expenses (alternate)

FROM fyyyy_f3