
WITH c2000_a AS (
    SELECT * FROM {{ source('bronze', 'c2000_a') }}
),

c2000_a_renamed AS (
    SELECT
         -- =====================
        -- Identifiers
        -- =====================
        CAST(2000 AS INTEGER) AS "YEAR",
        CAST(UNITID AS INTEGER) AS "UNITID",
        CIPCODE AS "CIPCODE",
        CAST(NULL AS INTEGER) AS "MAJORNUM",
        CAST(AWLEVEL AS INTEGER) AS "AWLEVEL",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST(CRACE15 AS INTEGER) + CAST(CRACE16 AS INTEGER) AS "CTOTALT",
        CAST(CRACE15 AS INTEGER) AS "CTOTALM",
        CAST(CRACE16 AS INTEGER) AS "CTOTALW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST(CRACE05 AS INTEGER) + CAST(CRACE06 AS INTEGER) AS "CAIANT",
        CAST(CRACE05 AS INTEGER) AS "CAIANM",
        CAST(CRACE06 AS INTEGER) AS "CAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST(CRACE07 AS INTEGER) + CAST(CRACE08 AS INTEGER) AS "CASIAT",
        CAST(CRACE07 AS INTEGER) AS "CASIAM",
        CAST(CRACE08 AS INTEGER) AS "CASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST(CRACE03 AS INTEGER) + CAST(CRACE04 AS INTEGER) AS "CBKAAT",
        CAST(CRACE03 AS INTEGER) AS "CBKAAM",
        CAST(CRACE04 AS INTEGER) AS "CBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST(CRACE09 AS INTEGER) + CAST(CRACE10 AS INTEGER) AS "CHISPT",
        CAST(CRACE09 AS INTEGER) AS "CHISPM",
        CAST(CRACE10 AS INTEGER) AS "CHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST(NULL AS INTEGER) AS "CNHPIT",
        CAST(NULL AS INTEGER) AS "CNHPIM",
        CAST(NULL AS INTEGER) AS "CNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST(CRACE11 AS INTEGER) + CAST(CRACE12 AS INTEGER) AS "CWHITT",
        CAST(CRACE11 AS INTEGER) AS "CWHITM",
        CAST(CRACE12 AS INTEGER) AS "CWHITW",

        -- =====================
        -- Two or More Races
        -- =====================
        CAST(NULL AS INTEGER) AS "C2MORT",
        CAST(NULL AS INTEGER) AS "C2MORM",
        CAST(NULL AS INTEGER) AS "C2MORW",

        -- =====================
        -- Unknown
        -- =====================
        CAST(CRACE13 AS INTEGER) + CAST(CRACE14 AS INTEGER) AS "CUNKNT",
        CAST(CRACE13 AS INTEGER) AS "CUNKNM",
        CAST(CRACE14 AS INTEGER) AS "CUNKNW",


        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST(CRACE01 AS INTEGER) + CAST(CRACE02 AS INTEGER) AS "CNRALT",
        CAST(CRACE01 AS INTEGER) AS "CNRALM",
        CAST(CRACE02 AS INTEGER) AS "CNRALW"

    FROM c2000_a
)

SELECT * FROM c2000_a_renamed