WITH ef2001a AS (
    SELECT * FROM {{ source('bronze', 'ef2001a') }}
),

ef2001a_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2001 AS INTEGER) AS "YEAR",
        CAST(UNITID AS INTEGER) AS "UNITID",
        CAST(NULL AS INTEGER) AS "EFALEVEL",
        CAST(LSTUDY AS INTEGER) AS "LSTUDY",

        -- Attendance Type
        CAST(SECTION AS INTEGER) AS "SECTION",

        -- =====================
        -- GRAND TOTALS (2001 DOES NOT HAVE TOTAL COLUMNS)
        -- We compute them ourselves:
        -- Men + Women = Total
        -- =====================
        (NULLIF(EFRACE15, '')::INTEGER + NULLIF(EFRACE16, '')::INTEGER) AS "EFTOTLT",
        CAST(EFRACE15 AS INTEGER) AS "EFTOTLM",
        CAST(EFRACE16 AS INTEGER) AS "EFTOTLW",
        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        (NULLIF(EFRACE05, '')::INTEGER + NULLIF(EFRACE06, '')::INTEGER) AS "EFAIANT",
        CAST(EFRACE05 AS INTEGER) AS "EFAIANM",
        CAST(EFRACE06 AS INTEGER) AS "EFAIANW",

        -- =====================
        -- Asian
        -- =====================
        (NULLIF(EFRACE07, '')::INTEGER + NULLIF(EFRACE08, '')::INTEGER) AS "EFASIAT",
        CAST(EFRACE07 AS INTEGER) AS "EFASIAM",
        CAST(EFRACE08 AS INTEGER) AS "EFASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        (NULLIF(EFRACE03, '')::INTEGER + NULLIF(EFRACE04, '')::INTEGER) AS "EFBKAAT",
        CAST(EFRACE03 AS INTEGER) AS "EFBKAAM",
        CAST(EFRACE04 AS INTEGER) AS "EFBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        (NULLIF(EFRACE09, '')::INTEGER + NULLIF(EFRACE10, '')::INTEGER) AS "EFHISPT",   -- correct total
        CAST(EFRACE09 AS INTEGER) AS "EFHISPM",
        CAST(EFRACE10 AS INTEGER) AS "EFHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- DOES NOT EXIST IN 2001 → SET NULL
        -- =====================
        NULL::INTEGER AS "EFNHPIT",
        NULL::INTEGER AS "EFNHPIM",
        NULL::INTEGER AS "EFNHPIW",

        -- =====================
        -- White
        -- =====================
        (NULLIF(EFRACE11, '')::INTEGER + NULLIF(EFRACE12, '')::INTEGER) AS "EFWHITT",
        CAST(EFRACE11 AS INTEGER) AS "EFWHITM",
        CAST(EFRACE12 AS INTEGER) AS "EFWHITW",

        -- =====================
        -- Two or More Races
        -- DOES NOT EXIST YET
        -- =====================
        NULL::INTEGER AS "EF2MORT",
        NULL::INTEGER AS "EF2MORM",
        NULL::INTEGER AS "EF2MORW",

        -- =====================
        -- Race Unknown
        -- =====================
        (NULLIF(EFRACE13, '')::INTEGER + NULLIF(EFRACE14, '')::INTEGER) AS "EFUNKNT",
        CAST(EFRACE13 AS INTEGER) AS "EFUNKNM",
        CAST(EFRACE14 AS INTEGER) AS "EFUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        (NULLIF(EFRACE02, '')::INTEGER + NULLIF(EFRACE01, '')::INTEGER) AS "EFNRALT",
        CAST(EFRACE01 AS INTEGER) AS "EFNRALW",
        CAST(EFRACE02 AS INTEGER) AS "EFNRALM",


        -- =====================
        -- Gender Other / Unknown
        -- NOT PRESENT IN 2001
        -- =====================
        CAST(NULL AS INTEGER) AS "EFGNDRUN"

    FROM ef2001a
)

SELECT *
FROM ef2001a_renamed
