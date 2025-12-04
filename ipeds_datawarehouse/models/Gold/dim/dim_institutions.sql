{{ config(
    materialized='table'
) }}

WITH ranked_inst AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "UNITID" ORDER BY "YEAR" DESC) AS rn
    FROM {{ ref('hdyyyy') }}
)

, latest_inst AS (
    SELECT *
    FROM ranked_inst 
    WHERE rn = 1
)

SELECT
    "YEAR" AS snapshot_year,  -- captures which year's record is used
    "UNITID" AS unit_id,
    COALESCE("INSTNM", 'N/A') AS institution_name,
    COALESCE("IALIAS", 'N/A') AS institution_alias,

    -- Location Details
    COALESCE("CITY", 'N/A') AS city,
    COALESCE("STABBR", 'N/A') AS state_abbreviation,
    COALESCE("ZIP", 'N/A') AS zip_code,
    "OBEREG" AS region_code,
    "LONGITUD" AS longitude,
    "LATITUDE" AS latitude,

    -- Institution Type & Control
    "SECTOR"AS sector,
    "ICLEVEL" AS institution_level,
    "CONTROL"AS control_type,

    -- Programs & Degrees
    "HLOFFER"AS highest_level_offering,
    "UGOFFER" AS offers_undergraduate,
    "GROFFER" AS offers_graduate,
    "HDEGOFR1" AS highest_degree_offered,

    -- Institution Size
    "INSTSIZE" AS institution_size,

    -- URLs
    COALESCE("WEBADDR", 'N/A') AS website_url,
    COALESCE("ADMINURL", 'N/A') AS admissions_url,
    COALESCE("FAIDURL", 'N/A') AS financial_aid_url

FROM latest_inst
