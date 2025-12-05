WITH base AS (
    SELECT 
        "YEAR" AS academic_year_id,
        "UNITID" AS unit_id,
        {{ clean_text('"INSTNM"') }} AS institution_name,
        {{ clean_text('"IALIAS"') }} AS alias_name,

        -- Institution type & control
        "SECTOR" AS sector,
        "ICLEVEL" AS institution_level,
        "CONTROL" AS control_type,

        -- Location details
        {{ clean_text('"CITY"') }} AS city,
        "STABBR" AS state_abbreviation,
        "ZIP" AS zip,
        "OBEREG" AS region_code,
        "LONGITUD" AS longitude,
        "LATITUDE" AS latitude,

        -- Programs & degrees
        "HLOFFER" AS highest_offering,
        "UGOFFER" AS offers_undergraduate,
        "GROFFER" AS offers_graduate,
        "HDEGOFR1" AS highest_degree_offered,

        -- Institution size
        "INSTSIZE" AS size_category,

        -- count non-null columns to choose fullest row
        (
            (CASE WHEN "INSTNM" IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN "CITY"   IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN "SECTOR" IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN "ZIP"    IS NOT NULL THEN 1 ELSE 0 END)
        ) AS completeness_score
    FROM {{ ref('hdyyyy') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY unit_id, academic_year_id
            ORDER BY completeness_score DESC
        ) AS rn
    FROM base
),

deduped AS (
    SELECT * 
    FROM ranked
    WHERE rn = 1
),

years AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
)

SELECT 
    d.academic_year_id,
    y.year_start_date,
    y.year_end_date,

    d.unit_id,
    d.institution_name,
    d.alias_name,
    d.city,
    d.state_abbreviation,
    d.region_code,
    d.longitude,
    d.latitude,

    d.sector,
    d.control_type,
    d.institution_level,

    d.highest_offering,
    d.highest_degree_offered,
    d.offers_undergraduate,
    d.offers_graduate,

    d.size_category

FROM deduped d
LEFT JOIN years y 
    ON (d.academic_year_id = y.academic_year_id)
