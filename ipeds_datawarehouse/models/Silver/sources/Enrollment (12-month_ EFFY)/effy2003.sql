 WITH effy2003 AS (
    SELECT * FROM {{ source('bronze', 'effy2003') }}
),

effy2003_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2003 AS INTEGER) AS "YEAR",
        CAST(UNITID AS INTEGER) AS "UNITID",
        CAST(NULL AS INTEGER) AS "EFFYALEV",
        CAST(EFFYLEV AS INTEGER) AS "EFFYLEV",
        CAST(LSTUDY AS INTEGER) AS "LSTUDY",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST(FYRACE24 AS INTEGER) AS "EFYTOTLT",
        CAST(FYRACE15 AS INTEGER) AS "EFYTOTLM",
        CAST(FYRACE16 AS INTEGER) AS "EFYTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        CAST(FYRACE19 AS INTEGER) AS "EFYAIANT",
        CAST(FYRACE05 AS INTEGER) AS "EFYAIANM",
        CAST(FYRACE06 AS INTEGER) AS "EFYAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST(FYRACE20 AS INTEGER) AS "EFYASIAT",
        CAST(FYRACE07 AS INTEGER) AS "EFYASIAM",
        CAST(FYRACE08 AS INTEGER) AS "EFYASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST(FYRACE18 AS INTEGER) AS "EFYBKAAT",
        CAST(FYRACE03 AS INTEGER) AS "EFYBKAAM",
        CAST(FYRACE04 AS INTEGER) AS "EFYBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        CAST(FYRACE21 AS INTEGER) AS "EFYHISPT",
        CAST(FYRACE09 AS INTEGER) AS "EFYHISPM",
        CAST(FYRACE10 AS INTEGER) AS "EFYHISPW",

        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST(NULL AS INTEGER) AS "EFYNHPIT",
        CAST(NULL AS INTEGER) AS "EFYNHPIM",
        CAST(NULL AS INTEGER) AS "EFYNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST(FYRACE22 AS INTEGER) AS "EFYWHITT",
        CAST(FYRACE11 AS INTEGER) AS "EFYWHITM",
        CAST(FYRACE12 AS INTEGER) AS "EFYWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST(NULL AS INTEGER) AS "EFY2MORT",
        CAST(NULL AS INTEGER) AS "EFY2MORM",
        CAST(NULL AS INTEGER) AS "EFY2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST(FYRACE23 AS INTEGER) AS "EFYUNKNT",
        CAST(FYRACE13 AS INTEGER) AS "EFYUNKNM",
        CAST(FYRACE14 AS INTEGER) AS "EFYUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST(FYRACE17 AS INTEGER) AS "EFYNRALT",
        CAST(FYRACE01 AS INTEGER) AS "EFYNRALW",
        CAST(FYRACE02 AS INTEGER) AS "EFYNRALM",

        -- =====================
        -- Gender Other / Unknown
        -- =====================
        CAST(NULL AS INTEGER) AS "EFYGUUN",
        CAST(NULL AS INTEGER) AS "EFYGUAN",
        CAST(NULL AS INTEGER) AS "EFYGUTOT"

    FROM effy2003
)

SELECT *
FROM effy2003_renamed
