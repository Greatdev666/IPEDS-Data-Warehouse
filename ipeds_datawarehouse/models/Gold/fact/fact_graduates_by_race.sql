{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "YEAR" AS year,
        "UNITID" AS unit_id,

        -- American Indian or Alaska Native
        SUM("GRAIANT") AS ai_an_total,
        SUM("GRAIANM") AS ai_an_male,
        SUM("GRAIANW") AS ai_an_female,

        -- Asian
        SUM("GRASIAT") AS asian_total,
        SUM("GRASIAM") AS asian_male,
        SUM("GRASIAW") AS asian_female,

        -- Black or African American
        SUM("GRBKAAT") AS black_total,
        SUM("GRBKAAM") AS black_male,
        SUM("GRBKAAW") AS black_female,

        -- Hispan)ic or Latino
        SUM("GRHISPT") AS hisp_total,
        SUM("GRHISPM") AS hisp_male,
        SUM("GRHISPW") AS hisp_female,

        -- Native Hawaiian or Pacific Islander
        SUM("GRNHPIT") AS nhpi_total,
        SUM("GRNHPIM") AS nhpi_male,
        SUM("GRNHPIW") AS nhpi_female,

        -- White
        SUM("GRWHITT") AS white_total,
        SUM("GRWHITM") AS white_male,
        SUM("GRWHITW") AS white_female,

        -- Two or More Races
        SUM("GR2MORT") AS multi_total,
        SUM("GR2MORM") AS multi_male,
        SUM("GR2MORW") AS multi_female,

        -- Nonresident Alien
        SUM("GRNRALT") AS nra_total,
        SUM("GRNRALM") AS nra_male,
        SUM("GRNRALW") AS nra_female,

        -- Unknown
        SUM("GRUNKNT") AS unk_total,
        SUM("GRUNKNM") AS unk_male,
        SUM("GRUNKNW") AS unk_female
    FROM {{ ref('gryyyy') }}
    GROUP BY 1,2
),
unpivoted AS (
    SELECT 
        year,
        unit_id,
        race_ethnicity_sk,
        total,
        male,
        female
    FROM base
    CROSS JOIN LATERAL (
        VALUES 
            (1, white_total, white_male, white_female),
            (2, black_total, black_male, black_female),
            (3, hisp_total, hisp_male, hisp_female),
            (4, asian_total, asian_male, asian_female),
            (5, ai_an_total, ai_an_male, ai_an_female),
            (6, nhpi_total, nhpi_male, nhpi_female),
            (7, multi_total, multi_male, multi_female),
            (8, nra_total, nra_male, nra_female),
            (9, unk_total, unk_male, unk_female)
    ) AS r (race_ethnicity_sk, total, male, female)
)

SELECT 
   year,
   unit_id,
   race_ethnicity_sk,
   COALESCE(total, 0) AS total,
   COALESCE(male, 0) AS male,
   COALESCE(female, 0) AS female
FROM unpivoted 