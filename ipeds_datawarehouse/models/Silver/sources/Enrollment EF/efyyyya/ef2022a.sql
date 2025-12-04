WITH ef2022a AS (
    SELECT * FROM {{ source('bronze', 'ef2022a') }}
),

ef2022a_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2022 AS INTEGER) AS "YEAR",
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
        CAST("EFTOTLT" AS INTEGER) AS "EFTOTLT",
        CAST("EFTOTLM" AS INTEGER) AS "EFTOTLM",
        CAST("EFTOTLW" AS INTEGER) AS "EFTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST("EFAIANT" AS INTEGER) AS "EFAIANT",
        CAST("EFAIANM" AS INTEGER) AS "EFAIANM",
        CAST("EFAIANW" AS INTEGER) AS "EFAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("EFASIAT" AS INTEGER) AS "EFASIAT",
        CAST("EFASIAM" AS INTEGER) AS "EFASIAM",
        CAST("EFASIAW" AS INTEGER) AS "EFASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("EFBKAAT" AS INTEGER) AS "EFBKAAT",
        CAST("EFBKAAM" AS INTEGER) AS "EFBKAAM",
        CAST("EFBKAAW" AS INTEGER) AS "EFBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST("EFHISPT" AS INTEGER) AS "EFHISPT",
        CAST("EFHISPM" AS INTEGER) AS "EFHISPM",
        CAST("EFHISPW" AS INTEGER) AS "EFHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST("EFNHPIT" AS INTEGER) AS "EFNHPIT",
        CAST("EFNHPIM" AS INTEGER) AS "EFNHPIM",
        CAST("EFNHPIW" AS INTEGER) AS "EFNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("EFWHITT" AS INTEGER) AS "EFWHITT",
        CAST("EFWHITM" AS INTEGER) AS "EFWHITM",
        CAST("EFWHITW" AS INTEGER) AS "EFWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST("EF2MORT" AS INTEGER) AS "EF2MORT",
        CAST("EF2MORM" AS INTEGER) AS "EF2MORM",
        CAST("EF2MORW" AS INTEGER) AS "EF2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST("EFUNKNT" AS INTEGER) AS "EFUNKNT",
        CAST("EFUNKNM" AS INTEGER) AS "EFUNKNM",
        CAST("EFUNKNW" AS INTEGER) AS "EFUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("EFNRALT" AS INTEGER) AS "EFNRALT",
        CAST("EFNRALM" AS INTEGER) AS "EFNRALM",
        CAST("EFNRALW" AS INTEGER) AS "EFNRALW",

        -- =====================
        -- Gender Other / Unknown
        -- =====================
        CAST("EFGNDRUN" AS INTEGER) AS "EFGNDRUN"

    FROM ef2022a
)

SELECT *
FROM ef2022a_renamed
