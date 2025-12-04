WITH ef2005a AS (
    SELECT * FROM {{ source('bronze', 'ef2005a') }}
),

ef2005a_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2005 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST("EFALEVEL" AS INTEGER) AS "EFALEVEL",
        CAST("LSTUDY" AS INTEGER) AS "LSTUDY",

        -- =====================
        -- Attendance Level Of Student
        -- =====================
        CAST("SECTION" AS INTEGER) AS "SECTION",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST("EFRACE24" AS INTEGER) AS "EFTOTLT",
        CAST("EFRACE15" AS INTEGER) AS "EFTOTLM",
        CAST("EFRACE16" AS INTEGER) AS "EFTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("EFRACE19" AS INTEGER) AS "EFAIANT",
        CAST("EFRACE05" AS INTEGER) AS "EFAIANM",
        CAST("EFRACE06" AS INTEGER) AS "EFAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("EFRACE20" AS INTEGER) AS "EFASIAT",
        CAST("EFRACE07" AS INTEGER) AS "EFASIAM",
        CAST("EFRACE08" AS INTEGER) AS "EFASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("EFRACE18" AS INTEGER) AS "EFBKAAT",
        CAST("EFRACE03" AS INTEGER) AS "EFBKAAM",
        CAST("EFRACE04" AS INTEGER) AS "EFBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("EFRACE21" AS INTEGER) AS "EFHISPT",
        CAST("EFRACE09" AS INTEGER) AS "EFHISPM",
        CAST("EFRACE10" AS INTEGER) AS "EFHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST(NULL AS INTEGER) AS "EFNHPIT",
        CAST(NULL AS INTEGER) AS "EFNHPIM",
        CAST(NULL AS INTEGER) AS "EFNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("EFRACE22" AS INTEGER) AS "EFWHITT",
        CAST("EFRACE11" AS INTEGER) AS "EFWHITM",
        CAST("EFRACE12" AS INTEGER) AS "EFWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST(NULL AS INTEGER) AS "EF2MORT",
        CAST(NULL AS INTEGER) AS "EF2MORM",
        CAST(NULL AS INTEGER) AS "EF2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST("EFRACE23" AS INTEGER) AS "EFUNKNT",
        CAST("EFRACE13" AS INTEGER) AS "EFUNKNM",
        CAST("EFRACE14" AS INTEGER) AS "EFUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("EFRACE17" AS INTEGER) AS "EFNRALT",
        CAST("EFRACE01" AS INTEGER) AS "EFNRALW",
        CAST("EFRACE02" AS INTEGER) AS "EFNRALM",

        -- =====================
        -- Gender Other / Unknown
        -- =====================
        CAST(NULL AS INTEGER) AS "EFGNDRUN"

    FROM ef2005a
)

SELECT *
FROM ef2005a_renamed
