{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "YEAR" AS year,
        "UNITID" AS unit_id,

        -- American Indian or Alaska Native
        SUM("CSAIANT") AS ai_an_total,
        SUM("CSAIANM") AS ai_an_male,
        SUM("CSAIANW") AS ai_an_female,

        -- Asian
        SUM("CSASIAT") AS asian_total,
        SUM("CSASIAM") AS asian_male,
        SUM("CSASIAW") AS asian_female,

        -- Black or African American
        SUM("CSBKAAT") AS black_total,
        SUM("CSBKAAM") AS black_male,
        SUM("CSBKAAW") AS black_female,

        -- Hispan)ic or Latino
        SUM("CSHISPT") AS hisp_total,
        SUM("CSHISPM") AS hisp_male,
        SUM("CSHISPW") AS hisp_female,

        -- Native Hawaiian or Pacific Islander
        SUM("CSNHPIT") AS nhpi_total,
        SUM("CSNHPIM") AS nhpi_male,
        SUM("CSNHPIW") AS nhpi_female,

        -- White
        SUM("CSWHITT") AS white_total,
        SUM("CSWHITM") AS white_male,
        SUM("CSWHITW") AS white_female,

        -- Two or More Races
        SUM("CS2MORT") AS multi_total,
        SUM("CS2MORM") AS multi_male,
        SUM("CS2MORW") AS multi_female,

        -- Nonresident Alien
        SUM("CSNRALT") AS nra_total,
        SUM("CSNRALM") AS nra_male,
        SUM("CSNRALW") AS nra_female,

        -- Unknown
        SUM("CSUNKNT") AS unk_total,
        SUM("CSUNKNM") AS unk_male,
        SUM("CSUNKNW") AS unk_female
    FROM {{ ref('cyyyy_b') }}
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