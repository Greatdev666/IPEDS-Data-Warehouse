WITH hd2017 AS (
    SELECT *
    FROM {{ source('bronze', 'hd2017') }}
),

hd2017_renamed AS (
    SELECT
        CAST(2017 AS INTEGER) AS "YEAR",

        -- =============================
        -- Institution Identity
        -- =============================
        CAST("UNITID" AS NUMERIC) AS "UNITID",
        CAST("INSTNM" AS TEXT) AS "INSTNM",
        CAST("IALIAS" AS TEXT) AS "IALIAS",

        -- =============================
        -- Location Details
        -- =============================
        CAST("CITY" AS TEXT) AS "CITY",
        CAST("STABBR" AS TEXT) AS "STABBR",
        CAST("ZIP" AS TEXT) AS "ZIP",
        CAST("OBEREG" AS NUMERIC) AS "OBEREG",
        CAST("LONGITUD" AS NUMERIC) AS "LONGITUD",
        CAST("LATITUDE" AS NUMERIC) AS "LATITUDE",

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
        CAST("HDEGOFR1" AS NUMERIC) AS "HDEGOFR1",

        -- =============================
        -- Institution Size
        -- =============================
        CAST("INSTSIZE" AS NUMERIC) AS "INSTSIZE",

        -- =============================
        -- URLs
        -- =============================
        CAST("WEBADDR" AS TEXT) AS "WEBADDR",
        CAST("ADMINURL" AS TEXT) AS "ADMINURL",
        CAST("FAIDURL" AS TEXT) AS "FAIDURL"

    FROM hd2017
)

SELECT *
FROM hd2017_renamed
