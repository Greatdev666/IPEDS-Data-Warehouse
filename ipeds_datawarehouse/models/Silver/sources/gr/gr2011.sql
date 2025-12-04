WITH gr2011 AS (
    SELECT * FROM {{ source('bronze', 'gr2011') }}
),

gr2011_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2011 AS INTEGER) AS "YEAR", 
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST("GRTYPE" AS INTEGER) AS "GRTYPE",
        CAST("CHRTSTAT" AS INTEGER) AS "CHRISTAT",
        CAST("SECTION" AS INTEGER) AS "SECTION",
        CAST("COHORT" AS INTEGER) AS "COHORT",
        CAST("LINE" AS TEXT) AS "LINE",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST("GRTOTLT" AS INTEGER) AS "GRTOTLT",
        CAST("GRTOTLM" AS INTEGER) AS "GRTOTLM",
        CAST("GRTOTLW" AS INTEGER) AS "GRTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("GRAIANT" AS INTEGER) AS "GRAIANT",
        CAST("GRAIANM" AS INTEGER) AS "GRAIANM",
        CAST("GRAIANW" AS INTEGER) AS "GRAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("GRASIAT" AS INTEGER) AS "GRASIAT",
        CAST("GRASIAM" AS INTEGER) AS "GRASIAM",
        CAST("GRASIAW" AS INTEGER) AS "GRASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("GRBKAAT" AS INTEGER) AS "GRBKAAT",
        CAST("GRBKAAM" AS INTEGER) AS "GRBKAAM",
        CAST("GRBKAAW" AS INTEGER) AS "GRBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("GRHISPT" AS INTEGER) AS "GRHISPT",
        CAST("GRHISPM" AS INTEGER) AS "GRHISPM",
        CAST("GRHISPW" AS INTEGER) AS "GRHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST("GRNHPIT" AS INTEGER) AS "GRNHPIT",
        CAST("GRNHPIM" AS INTEGER) AS "GRNHPIM",
        CAST("GRNHPIW" AS INTEGER) AS "GRNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("GRWHITT" AS INTEGER) AS "GRWHITT",
        CAST("GRWHITM" AS INTEGER) AS "GRWHITM",
        CAST("GRWHITW" AS INTEGER) AS "GRWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST("GR2MORT" AS INTEGER) AS "GR2MORT",
        CAST("GR2MORM" AS INTEGER) AS "GR2MORM",
        CAST("GR2MORW" AS INTEGER) AS "GR2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST("GRUNKNT" AS INTEGER) AS "GRUNKNT",
        CAST("GRUNKNM" AS INTEGER) AS "GRUNKNM",
        CAST("GRUNKNW" AS INTEGER) AS "GRUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("GRNRALT" AS INTEGER) AS "GRNRALT",
        CAST("GRNRALM" AS INTEGER) AS "GRNRALM",
        CAST("GRNRALW" AS INTEGER) AS "GRNRALW"

    FROM gr2011
)

SELECT *
FROM gr2011_renamed
