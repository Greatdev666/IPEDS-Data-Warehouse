WITH hd2004 AS (
    SELECT *
    FROM {{ source('bronze', 'hd2004') }}
),

hd2004_renamed AS (
    SELECT
        CAST(2004 AS INTEGER) AS "YEAR",

        -- =============================
        -- Institution Identity
        -- =============================
        CAST("UNITID" AS NUMERIC) AS "UNITID",
        CAST("INSTNM" AS TEXT) AS "INSTNM",
        CAST(NULL AS TEXT) AS "IALIAS",

        -- =============================
        -- Location Details
        -- =============================
        CAST("CITY" AS TEXT) AS "CITY",
        CAST("STABBR" AS TEXT) AS "STABBR",
        CAST("ZIP" AS TEXT) AS "ZIP",
        CAST("OBEREG" AS NUMERIC) AS "OBEREG",
        CAST(NULL AS NUMERIC) AS "LONGITUD",
        CAST(NULL AS NUMERIC) AS "LATITUDE",

        -- =============================
        -- Institution Type & Control
        -- =============================
        CAST("SECTOR" AS NUMERIC) AS "SECTOR",
        CAST("ICLEVEL" AS NUMERIC) AS "ICLEVEL",
        CAST("CONTROL" AS NUMERIC) AS "CONTROL",

        -- =============================
        -- Programs & Degrees
        -- =============================
        CAST("HLOFFER" AS NUMERIC) AS "HLOFFER",
        CAST("UGOFFER" AS NUMERIC) AS "UGOFFER",
        CAST("GROFFER" AS NUMERIC) AS "GROFFER",
        CAST(NULL AS NUMERIC) AS "HDEGOFR1",

        -- =============================
        -- Institution Size
        -- =============================
        CAST(NULL AS NUMERIC) AS "INSTSIZE",

        -- =============================
        -- URLs
        -- =============================
        CAST("WEBADDR" AS TEXT) AS "WEBADDR",
        CAST(NULL AS TEXT) AS "ADMINURL",
        CAST(NULL AS TEXT) AS "FAIDURL"

    FROM hd2004
)

SELECT * FROM hd2004_renamed
