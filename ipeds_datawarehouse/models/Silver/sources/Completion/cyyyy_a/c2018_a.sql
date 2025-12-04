
WITH c2018_a AS (
    SELECT * FROM {{ source('bronze', 'c2018_a') }}
),

c2018_a_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2018 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        "CIPCODE",
        CAST("MAJORNUM" AS INTEGER) AS "MAJORNUM",
        CAST("AWLEVEL" AS INTEGER) AS "AWLEVEL",
        -- =====================
        -- Grand Totals
        -- =====================
        CAST("CTOTALT" AS INTEGER) AS "CTOTALT",
        CAST("CTOTALM" AS INTEGER) AS "CTOTALM",
        CAST("CTOTALW" AS INTEGER) AS "CTOTALW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("CAIANT" AS INTEGER) AS "CAIANT",
        CAST("CAIANM" AS INTEGER) AS "CAIANM",
        CAST("CAIANW" AS INTEGER) AS "CAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("CASIAT" AS INTEGER) AS "CASIAT",
        CAST("CASIAM" AS INTEGER) AS "CASIAM",
        CAST("CASIAW" AS INTEGER) AS "CASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("CBKAAT" AS INTEGER) AS "CBKAAT",
        CAST("CBKAAM" AS INTEGER) AS "CBKAAM",
        CAST("CBKAAW" AS INTEGER) AS "CBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("CHISPT" AS INTEGER) AS "CHISPT",
        CAST("CHISPM" AS INTEGER) AS "CHISPM",
        CAST("CHISPW" AS INTEGER) AS "CHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST("CNHPIT" AS INTEGER) AS "CNHPIT",
        CAST("CNHPIM" AS INTEGER) AS "CNHPIM",
        CAST("CNHPIW" AS INTEGER) AS "CNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("CWHITT" AS INTEGER) AS "CWHITT",
        CAST("CWHITM" AS INTEGER) AS "CWHITM",
        CAST("CWHITW" AS INTEGER) AS "CWHITW",

        -- =====================
        -- Two or More Races
        -- =====================
        CAST("C2MORT" AS INTEGER) AS "C2MORT",
        CAST("C2MORM" AS INTEGER) AS "C2MORM",
        CAST("C2MORW" AS INTEGER) AS "C2MORW",

        -- =====================
        -- Race Unknown
        -- =====================
        CAST("CUNKNT" AS INTEGER) AS "CUNKNT",
        CAST("CUNKNM" AS INTEGER) AS "CUNKNM",
        CAST("CUNKNW" AS INTEGER) AS "CUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("CNRALT" AS INTEGER) AS "CNRALT",
        CAST("CNRALM" AS INTEGER) AS "CNRALM",
        CAST("CNRALW" AS INTEGER) AS "CNRALW"

    FROM c2018_a
)

SELECT * FROM c2018_a_renamed

