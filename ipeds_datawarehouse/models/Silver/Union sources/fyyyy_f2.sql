WITH fyyyy_f2 AS (
    SELECT * FROM {{ ref('f2023f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2022f2') }}
    UNION ALL 
    SELECT * FROM {{ ref('f2021f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2020f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2019f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2018f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2017f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2016f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2015f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2014f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2013f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2012f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2011f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2010f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2009f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2008f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2007f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2006f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2005f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2004f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2003f2') }}
    UNION ALL
    SELECT * FROM {{ ref('f2002f2') }}
)

SELECT 
    "YEAR",
        -- =====================
        -- Identifiers
        -- =====================
        "UNITID",        -- Unique identification number of the institution

        -- =====================
        -- Assets
        -- =====================
        "F2A01",        -- Long-term investments
        "F2A02",         -- Total assets
        "F2A03",          -- Total Liabilities
        "F2A04",         -- Total unrestricted net assets
        "F2A05",         -- Total restricted net assets
        "F2A06",         -- Total net assets
        "F2A11",         -- Land improvements - End of year
        "F2A12",         -- Buildings - End of year
        "F2A13",         -- Equipment, including art and library collections - End of year
        "F2A15",         -- Construction in Progress
        "F2A17",         -- Total Plant, Property, and Equipment
        "F2A18",         -- Accumulated depreciation
        "F2A19",         -- Property, Plant, and Equipment, net of accumulated depreciation
        "F2A20",         -- Intangible Assets, net of accumulated amortization

        -- =====================
        -- Revenues
        -- =====================
        "F2B01",         -- Total revenues and investment return
        "F2D01",         -- Tuition and fees - Total
        "F2D02",         -- Federal appropriations - Total
        "F2D03",         -- State appropriations - Total
        "F2D04",         -- Local appropriations - Total
        "F2D05",         -- Federal grants & contracts - Total
        "F2D06",         -- State grants & contracts - Total
        "F2D07",         -- Local grants & contracts - Total
        "F2D08",         -- Private gifts, grants & contracts - Total
        "F2D09",         -- Contributions from affiliated entities - Total
        "F2D10",         -- Investment return - Total
        "F2D11",         -- Sales and services of educational activities - Total
        "F2D12",         -- Sales and services of auxiliary enterprises - Total
        "F2D13",         -- Hospital revenue - Total
        "F2D14",         -- Independent operations revenue - Total
        "F2D15",         -- Other revenue - Total
        "F2D16",         -- Total revenues and investment return - Total
        "F2D17",         -- Net assets released from restriction - Total
        "F2D18",         -- Net total revenues, after assets released from restriction - Total

        -- =====================
        -- Expenses
        -- =====================
        "F2B02",         -- Total expenses
        "F2E011",        -- Instruction - Total
        "F2E021",        -- Research - Total
        "F2E031",        -- Public service - Total
        "F2E041",        -- Academic support - Total
        "F2E051",        -- Student service - Total
        "F2E061",        -- Institutional support - Total
        "F2E071",        -- Auxiliary enterprises - Total
        "F2E081",        -- Net grant aid to students - Total
        "F2E091",        -- Hospital services - Total
        "F2E101",        -- Independent operations - Total
        "F2E121",        -- Other expenses - Total
        "F2E131",        -- Total expenses - Total
        "F2E135",        -- Total expenses - Depreciation
        "F2E136",        -- Total expenses - Interest
        -- =====================
        -- Endowment
        -- =====================
        "F2H01",         -- Value of endowment assets at the beginning of the fiscal year
        "F2H02",         -- Value of endowment assets at the end of the fiscal year
        "F2H03",         -- Change in value of endowment net assets
        "F2H03A",        -- New gifts and additions
        "F2H03B",        -- Endowment net investment return
        "F2H03C",        -- Spending distribution for current use
        "F2H03D"        -- Other changes in value of endowment net assets

FROM fyyyy_f2