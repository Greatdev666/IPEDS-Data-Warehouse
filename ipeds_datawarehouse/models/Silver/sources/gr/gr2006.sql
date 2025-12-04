WITH gr2006 AS (
    SELECT * FROM {{ source('bronze', 'gr2006') }}
),

gr2006_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2006 AS INTEGER) AS "YEAR", 
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST("GRTYPE" AS INTEGER) AS "GRTYPE",
        CAST("CHRTSTAT" AS INTEGER) AS "CHRISTAT",
        CAST("SECTION" AS INTEGER) AS "SECTION",
        CAST("COHORT" AS INTEGER) AS "COHORT",
        CAST("LINE" AS TEXT) AS "LINE",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST("GRRACE24" AS INTEGER) AS "GRTOTLT",
        CAST("GRRACE15" AS INTEGER) AS "GRTOTLM",
        CAST("GRRACE16" AS INTEGER) AS "GRTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("GRRACE19" AS INTEGER) AS "GRAIANT",
        CAST("GRRACE05" AS INTEGER) AS "GRAIANM",
        CAST("GRRACE06" AS INTEGER) AS "GRAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("GRRACE20" AS INTEGER) AS "GRASIAT",
        CAST("GRRACE07" AS INTEGER) AS "GRASIAM",
        CAST("GRRACE08" AS INTEGER) AS "GRASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("GRRACE18" AS INTEGER) AS "GRBKAAT",
        CAST("GRRACE03" AS INTEGER) AS "GRBKAAM",
        CAST("GRRACE04" AS INTEGER) AS "GRBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("GRRACE21" AS INTEGER) AS "GRHISPT",
        CAST("GRRACE09" AS INTEGER) AS "GRHISPM",
        CAST("GRRACE10" AS INTEGER) AS "GRHISPW",

        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST(NULL AS INTEGER) AS "GRNHPIT",
        CAST(NULL AS INTEGER) AS "GRNHPIM",
        CAST(NULL AS INTEGER) AS "GRNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("GRRACE22" AS INTEGER) AS "GRWHITT",
        CAST("GRRACE11" AS INTEGER) AS "GRWHITM",
        CAST("GRRACE12" AS INTEGER) AS "GRWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST(NULL AS INTEGER) AS "GR2MORT",
        CAST(NULL AS INTEGER) AS "GR2MORM",
        CAST(NULL AS INTEGER) AS "GR2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST("GRRACE23" AS INTEGER) AS "GRUNKNT",
        CAST("GRRACE13" AS INTEGER) AS "GRUNKNM",
        CAST("GRRACE14" AS INTEGER) AS "GRUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("GRRACE17" AS INTEGER) AS "GRNRALT",
        CAST("GRRACE01" AS INTEGER) AS "GRNRALW",
        CAST("GRRACE02" AS INTEGER) AS "GRNRALM"

    FROM gr2006
)

SELECT *
FROM gr2006_renamed
