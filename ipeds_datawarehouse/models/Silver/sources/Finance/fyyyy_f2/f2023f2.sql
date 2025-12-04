WITH f2023_f2 AS (
    SELECT * FROM {{ source('bronze', 'f2023_f2') }} 
)
, f2023_f2_renamed AS (

    SELECT
        CAST(2023 AS INT) AS "YEAR",
        -- =====================
        -- Identifiers
        -- =====================
        CAST("UNITID" AS INT) AS "UNITID",

        -- =====================
        -- Assets
        -- =====================
        CAST("F2A01" AS NUMERIC) AS "F2A01",          -- Long-term investments
        CAST("F2A02" AS NUMERIC) AS "F2A02",          -- Total assets
        CAST("F2A03" AS NUMERIC) AS "F2A03",          -- Total Liabilities
        CAST("F2A04" AS NUMERIC) AS "F2A04",          -- Total unrestricted net assets
        CAST("F2A05" AS NUMERIC) AS "F2A05",          -- Total restricted net assets
        CAST("F2A06" AS NUMERIC) AS "F2A06",          -- Total net assets
        CAST("F2A11" AS NUMERIC) AS "F2A11",          -- Land improvements
        CAST("F2A12" AS NUMERIC) AS "F2A12",          -- Buildings
        CAST("F2A13" AS NUMERIC) AS "F2A13",          -- Equipment
        CAST("F2A15" AS NUMERIC) AS "F2A15",          -- Construction in Progress
        CAST("F2A17" AS NUMERIC) AS "F2A17",          -- Total Plant, Property, and Equipment
        CAST("F2A18" AS NUMERIC) AS "F2A18",          -- Accumulated depreciation
        CAST("F2A19" AS NUMERIC) AS "F2A19",          -- Property, Plant, and Equipment, net
        CAST("F2A20" AS NUMERIC) AS "F2A20",          -- Intangible Assets, net

        -- =====================
        -- Revenues
        -- =====================
        CAST("F2B01" AS NUMERIC) AS "F2B01",          -- Total revenues and investment return
        CAST("F2D01" AS NUMERIC) AS "F2D01",          -- Tuition and fees - Total
        CAST("F2D02" AS NUMERIC) AS "F2D02",          -- Federal appropriations - Total
        CAST("F2D03" AS NUMERIC) AS "F2D03",          -- State appropriations - Total
        CAST("F2D04" AS NUMERIC) AS "F2D04",          -- Local appropriations - Total
        CAST("F2D05" AS NUMERIC) AS "F2D05",          -- Federal grants & contracts - Total
        CAST("F2D06" AS NUMERIC) AS "F2D06",          -- State grants & contracts - Total
        CAST("F2D07" AS NUMERIC) AS "F2D07",          -- Local grants & contracts - Total
        CAST("F2D08" AS NUMERIC) AS "F2D08",          -- Private gifts, grants & contracts - Total
        CAST("F2D09" AS NUMERIC) AS "F2D09",          -- Contributions from affiliated entities - Total
        CAST("F2D10" AS NUMERIC) AS "F2D10",          -- Investment return - Total
        CAST("F2D11" AS NUMERIC) AS "F2D11",          -- Sales and services of educational activities - Total
        CAST("F2D12" AS NUMERIC) AS "F2D12",          -- Sales and services of auxiliary enterprises - Total
        CAST("F2D13" AS NUMERIC) AS "F2D13",          -- Hospital revenue - Total
        CAST("F2D14" AS NUMERIC) AS "F2D14",          -- Independent operations revenue - Total
        CAST("F2D15" AS NUMERIC) AS "F2D15",          -- Other revenue - Total
        CAST("F2D16" AS NUMERIC) AS "F2D16",          -- Total revenues and investment return - Total
        CAST("F2D17" AS NUMERIC) AS "F2D17",          -- Net assets released from restriction - Total
        CAST("F2D18" AS NUMERIC) AS "F2D18",          -- Net total revenues, after assets released from restriction - Total

        -- =====================
        -- Expenses
        -- =====================
        CAST("F2B02" AS NUMERIC) AS "F2B02",         -- Total expenses
        CAST("F2E011" AS NUMERIC) AS "F2E011",       -- Instruction - Total
        CAST("F2E021" AS NUMERIC) AS "F2E021",       -- Research - Total
        CAST("F2E031" AS NUMERIC) AS "F2E031",       -- Public service - Total
        CAST("F2E041" AS NUMERIC) AS "F2E041",       -- Academic support - Total
        CAST("F2E051" AS NUMERIC) AS "F2E051",       -- Student service - Total
        CAST("F2E061" AS NUMERIC) AS "F2E061",       -- Institutional support - Total
        CAST("F2E071" AS NUMERIC) AS "F2E071",       -- Auxiliary enterprises - Total
        CAST("F2E081" AS NUMERIC) AS "F2E081",       -- Net grant aid to students - Total
        CAST("F2E091" AS NUMERIC) AS "F2E091",       -- Hospital services - Total
        CAST("F2E101" AS NUMERIC) AS "F2E101",       -- Independent operations - Total
        CAST("F2E121" AS NUMERIC) AS "F2E121",       -- Other expenses - Total
        CAST("F2E131" AS NUMERIC) AS "F2E131",       -- Total expenses - Total
        CAST("F2E135" AS NUMERIC) AS "F2E135",       -- Total expenses - Depreciation
        CAST("F2E136" AS NUMERIC) AS "F2E136",       -- Total expenses - Interest
        -- =====================
        -- Endowment
        -- =====================
        CAST("F2H01" AS NUMERIC) AS "F2H01",         -- Endowment start
        CAST("F2H02" AS NUMERIC) AS "F2H02",         -- Endowment end
        CAST("F2H03" AS NUMERIC) AS "F2H03",         -- Change in endowment net assets
        CAST("F2H03A" AS NUMERIC) AS "F2H03A",       -- New gifts and additions
        CAST("F2H03B" AS NUMERIC) AS "F2H03B",       -- Endowment net investment return
        CAST("F2H03C" AS NUMERIC) AS "F2H03C",       -- Spending distribution for current use
        CAST("F2H03D" AS NUMERIC) AS "F2H03D"        -- Other changes in value of endowment net assets

    FROM f2023_f2

)

SELECT * FROM f2023_f2_renamed
