WITH ef2001b AS (
    SELECT * FROM {{ source('bronze', 'ef2001b') }}
)
, ef2001b_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2001 AS INTEGER) AS "YEAR",
        CAST(UNITID AS INTEGER) AS "UNITID",
        CAST(LSTUDY AS INTEGER) AS "LSTUDY",
        CAST(LINE AS INTEGER) AS "LINE",

        -- =====================
        -- Age Category of Student
        -- =====================
        CAST(NULL AS INTEGER) AS "EFBAGE",

       -- =====================
        -- Grand Totals
        -- =====================
        (NULLIF(EFAGE01, '')::INTEGER + NULLIF(EFAGE02, '')::INTEGER) AS "EFAGE09",
        CAST(EFAGE01 AS INTEGER) AS "EFAGE07",
        CAST(EFAGE02 AS INTEGER) AS "EFAGE08",

        -- =====================
        -- Full Time Students
        -- =====================
        CAST(NULL AS INTEGER) AS "EFAGE05",
        CAST(NULL AS INTEGER) AS "EFAGE01",
        CAST(NULL AS INTEGER) AS "EFAGE02",

        -- =====================
        -- Full Time Students
        -- =====================
        CAST(NULL AS INTEGER) AS "EFAGE06",
        CAST(NULL AS INTEGER) AS "EFAGE03",
        CAST(NULL AS INTEGER) AS "EFAGE04"
        
    FROM ef2001b
)

SELECT * FROM ef2001b_renamed