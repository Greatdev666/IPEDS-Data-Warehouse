WITH effy2009 AS (
    SELECT * FROM {{ source('bronze', 'effy2009') }}
),

effy2009_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2009 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST(NULL AS INTEGER) AS "EFFYALEV",
        CAST("EFFYLEV" AS INTEGER) AS "EFFYLEV",
        CAST("LSTUDY" AS INTEGER) AS "LSTUDY",

        -- =====================
        -- Grand Totals
        -- =====================
        CAST("EFYTOTLT" AS INTEGER) AS "EFYTOTLT",
        CAST("EFYTOTLM" AS INTEGER) AS "EFYTOTLM",
        CAST("EFYTOTLW" AS INTEGER) AS "EFYTOTLW",

        -- =====================
        -- American Indian / Alaska Native
        -- =====================
        COALESCE(NULLIF("EFYAIANT", '')::INTEGER, NULLIF("FYRACE19", '')::INTEGER ) AS "EFYAIANT",
        COALESCE(NULLIF("EFYAIANM", '')::INTEGER, NULLIF("EFYAIANM", '')::INTEGER) AS "EFYAIANM",
        COALESCE(NULLIF("EFYAIANW", '')::INTEGER, NULLIF("EFYAIANW", '')::INTEGER) AS "EFYAIANW",

        -- =====================
        -- Asian
        -- =====================
        CAST("EFYASIAT" AS INTEGER) AS "EFYASIAT",
        CAST("EFYASIAM" AS INTEGER) AS "EFYASIAM",
        CAST("EFYASIAW" AS INTEGER) AS "EFYASIAW",

        -- =====================
        -- Black / African American
        -- =====================
        CAST("EFYBKAAT" AS INTEGER) AS "EFYBKAAT",
        CAST("EFYBKAAM" AS INTEGER) AS "EFYBKAAM",
        CAST("EFYBKAAW" AS INTEGER) AS "EFYBKAAW",

        -- =====================
        -- Hispanic / Latino
        -- =====================
        COALESCE(NULLIF("EFYHISPT", '')::INTEGER, NULLIF("FYRACE21", '')::INTEGER ) AS "EFYHISPT",
        COALESCE(NULLIF("EFYHISPM", '')::INTEGER, NULLIF("FYRACE09", '')::INTEGER) AS "EFYHISPM",
        COALESCE(NULLIF("EFYHISPW", '')::INTEGER, NULLIF("FYRACE10", '')::INTEGER) AS "EFYHISPW",

        -- =====================
        -- Native Hawaiian / Pacific Islander
        -- =====================
        CAST("EFYNHPIT" AS INTEGER) AS "EFYNHPIT",
        CAST("EFYNHPIM" AS INTEGER) AS "EFYNHPIM",
        CAST("EFYNHPIW" AS INTEGER) AS "EFYNHPIW",

        -- =====================
        -- White
        -- =====================
        CAST("EFYWHITT" AS INTEGER) AS "EFYWHITT",
        CAST("EFYWHITM" AS INTEGER) AS "EFYWHITM",
        CAST("EFYWHITW" AS INTEGER) AS "EFYWHITW",

        -- =====================
        -- Two or more races
        -- =====================
        CAST("EFY2MORT" AS INTEGER) AS "EFY2MORT",
        CAST("EFY2MORM" AS INTEGER) AS "EFY2MORM",
        CAST("EFY2MORW" AS INTEGER) AS "EFY2MORW",

        -- =====================
        -- Race unknown
        -- =====================
        CAST("EFYUNKNT" AS INTEGER) AS "EFYUNKNT",
        CAST("EFYUNKNM" AS INTEGER) AS "EFYUNKNM",
        CAST("EFYUNKNW" AS INTEGER) AS "EFYUNKNW",

        -- =====================
        -- Nonresident Alien
        -- =====================
        CAST("EFYNRALT" AS INTEGER) AS "EFYNRALT",
        CAST("EFYNRALM" AS INTEGER) AS "EFYNRALM",
        CAST("EFYNRALW" AS INTEGER) AS "EFYNRALW",

        -- =====================
        -- Gender Other / Unknown
        -- =====================
        CAST(NULL AS INTEGER) AS "EFYGUUN",
        CAST(NULL AS INTEGER) AS "EFYGUAN",
        CAST(NULL AS INTEGER) AS "EFYGUTOT"

    FROM effy2009
)

SELECT *
FROM effy2009_renamed
