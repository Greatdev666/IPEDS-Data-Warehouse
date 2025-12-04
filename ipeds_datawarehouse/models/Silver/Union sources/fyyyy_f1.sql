WITH fyyyy_f1 AS (
    SELECT * FROM {{ ref('f2023f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2022f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2021f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2020f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2019f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2018f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2017f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2016f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2015f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2014f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2013f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2012f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2011f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2010f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2009f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2008f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2007f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2006f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2005f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2004f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2003f1') }}
    UNION ALL
    SELECT * FROM {{ ref('f2002f1') }}
)

SELECT 
    "YEAR",          

    -- =====================
    -- Identity
    -- =====================
    "UNITID",        -- Unique identification number of the institution

    -- =====================
    -- Assets & Liabilities
    -- =====================
    "F1A01",         -- Total current assets
    "F1A05",         -- Total noncurrent assets
    "F1A06",         -- Total assets
    "F1A19",         -- Deferred outflows of resources
    "F1A09",         -- Total current liabilities
    "F1A12",         -- Total noncurrent liabilities
    "F1A13",         -- Total liabilities
    "F1A20",         -- Deferred inflows of resources

    -- =====================
    -- Net Position
    -- =====================
    "F1A14",         -- Invested in capital assets, net of related debt
    "F1A15",         -- Restricted-expendable net position
    "F1A16",         -- Restricted-nonexpendable net position
    "F1A17",         -- Unrestricted net position
    "F1A18",         -- Net position end of year
    -- =====================
    -- Property, Plant & Equipment (PPE)
    -- =====================
    "F1A27T4",       -- Total for plant, property, and equipment - ending balance
    "F1A284",        -- Accumulated depreciation - ending balance
    "F1A274",        -- Construction in progress - ending balance

    -- =====================
    -- Operating Revenues
    -- =====================
    "F1B01",         -- Tuition and fees, after discounts and allowances
    "F1B02",         -- Federal operating grants and contracts
    "F1B03",         -- State operating grants and contracts
    "F1B04B",        -- Private operating grants and contracts
    "F1B05",         -- Sales and services of auxiliary enterprises
    "F1B09",         -- Total operating revenues
    

    -- =====================
    -- Non-operating Revenues
    -- =====================
    "F1B10",         -- Federal appropriations
    "F1B11",         -- State appropriations
    "F1B13",         -- Federal nonoperating grants
    "F1B16",         -- Gifts, including contributions from affiliated organizations
    "F1B17",         -- Investment income
    "F1B19",         -- Total nonoperating revenues
    "F1B27",         -- Total operating + nonoperating revenues
    "F1B25",         -- Total all revenues and other additions

    -- =====================
    -- Functional Expenses
    -- =====================
    "F1C011",        -- Instruction - current year total
    "F1C021",        -- Research - current year total
    "F1C031",        -- Public service - current year total
    "F1C051",        -- Academic support - current year total
    "F1C061",        -- Student services - current year total
    "F1C071",        -- Institutional support - current year total
    "F1C101",        -- Scholarships and fellowships - current year total
    "F1C111",        -- Auxiliary enterprises - current year total
    "F1C141",        -- Other expenses/deductions - current year total
    "F1C191",        -- Total expenses and deductions - current year total
    "F1B26",        -- sales_educational_services
    "F1B06",         -- hospital_revenue
    -- =====================
    -- Pension / OPEB
    -- =====================
    "F1M01",         -- Pension expense
    "F1M02",         -- Net pension liability
    "F1M05",         -- Other postemployment benefit (OPEB) expense
    "F1M06",         -- Other postemployment benefit (OPEB) net liability

    -- =====================
    -- Scholarships & Discounts
    -- =====================
    "F1E01",         -- Pell grants (federal)
    "F1E07",         -- Total gross scholarships and fellowships
    "F1E10",         -- Total discounts and allowances
    "F1E11",         -- Net scholarships and fellowship expenses

    -- =====================
    -- Endowment
    -- =====================
    "F1H01",         -- Value of endowment assets at the beginning of the fiscal year
    "F1H02",         -- Value of endowment assets at the end of the fiscal year
    "F1H03"         -- Change in value of endowment net assets


FROM fyyyy_f1