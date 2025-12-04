WITH ef2018b AS (
    SELECT * FROM {{ source('bronze', 'ef2018b') }}
)
, ef2018b_renamed AS (
    SELECT
        -- =====================
        -- Identifiers
        -- =====================
        CAST(2018 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST("LSTUDY" AS INTEGER) AS "LSTUDY",
        CAST("LINE" AS INTEGER) AS "LINE",

        -- =====================
        -- Age Category of Student
        -- =====================
        CAST("EFBAGE" AS INTEGER) AS "EFBAGE",

       -- =====================
        -- Grand Totals
        -- =====================
        CAST("EFAGE09" AS INTEGER) AS "EFAGE09",
        CAST("EFAGE07" AS INTEGER) AS "EFAGE07",
        CAST("EFAGE08" AS INTEGER) AS "EFAGE08",

        -- =====================
        -- Full Time Students
        -- =====================
        CAST("EFAGE05" AS INTEGER) AS "EFAGE05",
        CAST("EFAGE01" AS INTEGER) AS "EFAGE01",
        CAST("EFAGE02" AS INTEGER) AS "EFAGE02",

        -- =====================
        -- Full Time Students
        -- =====================
        CAST("EFAGE06" AS INTEGER) AS "EFAGE06",
        CAST("EFAGE03" AS INTEGER) AS "EFAGE03",
        CAST("EFAGE04" AS INTEGER) AS "EFAGE04"
        
    FROM ef2018b
)

SELECT * FROM ef2018b_renamed