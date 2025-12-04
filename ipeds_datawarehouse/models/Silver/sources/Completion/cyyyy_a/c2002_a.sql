

-- FILE: completions_2002_2002.sql

WITH c2002_a AS (
    SELECT * FROM {{ source('bronze', 'c2002_a') }}
),

c2002_a_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2002 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        "CIPCODE",
        CAST("MAJORNUM" AS INTEGER) AS "MAJORNUM",
        CAST("AWLEVEL" AS INTEGER) AS "AWLEVEL",
        -- =====================
        -- Grand Totals
        -- =====================
        CAST("CRACE24" AS INTEGER) AS "CTOTALT",
        CAST("CRACE15" AS INTEGER) AS "CTOTALM",
        CAST("CRACE16" AS INTEGER) AS "CTOTALW", 

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("CRACE19" AS INTEGER) AS "CAIANT",
        CAST("CRACE05" AS INTEGER) AS "CAIANM",
        CAST("CRACE06" AS INTEGER) AS "CAIANW",

        -- =====================
        -- Asian / Pacific Islander
        -- =====================
        CAST("CRACE20" AS INTEGER) AS "CASIAT",
        CAST("CRACE07" AS INTEGER) AS "CASIAM",
        CAST("CRACE08" AS INTEGER) AS "CASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("CRACE18" AS INTEGER) AS "CBKAAT",
        CAST("CRACE03" AS INTEGER) AS "CBKAAM",
        CAST("CRACE04" AS INTEGER) AS "CBKAAW",


        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("CRACE21" AS INTEGER) AS "CHISPT",
        CAST("CRACE09" AS INTEGER) AS "CHISPM",
        CAST("CRACE10" AS INTEGER) AS "CHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST(NULL AS INTEGER) AS "CNHPIT",
        CAST(NULL AS INTEGER) AS "CNHPIM",
        CAST(NULL AS INTEGER) AS "CNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("CRACE22" AS INTEGER) AS "CWHITT",
        CAST("CRACE11" AS INTEGER) AS "CWHITM",
        CAST("CRACE12" AS INTEGER) AS "CWHITW",

        -- =====================
        -- Two or More Races
        -- =====================
        CAST(NULL AS INTEGER) AS "C2MORT",
        CAST(NULL AS INTEGER) AS "C2MORM",
        CAST(NULL AS INTEGER) AS "C2MORW",

        -- =====================
        -- Unknown
        -- =====================
        CAST("CRACE23" AS INTEGER) AS "CUNKT",
        CAST("CRACE13" AS INTEGER) AS "CUNKNM",
        CAST("CRACE14" AS INTEGER) AS "CUNKNW", 

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("CRACE17" AS INTEGER) AS "CNRALT",
        CAST("CRACE01" AS INTEGER) AS "CNRALM",
        CAST("CRACE02" AS INTEGER) AS "CNRALW"

    FROM c2002_a
)

SELECT * FROM c2002_a_renamed