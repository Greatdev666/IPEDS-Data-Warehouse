{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "YEAR" AS year,
        "UNITID" AS unit_id,

        -- American Indian or Alaska Native
        SUM("EFYAIANT") AS ai_an_total,
        SUM("EFYAIANM") AS ai_an_male,
        SUM("EFYAIANW") AS ai_an_female,

        -- Asian
        SUM("EFYASIAT") AS asian_total,
        SUM("EFYASIAM") AS asian_male,
        SUM("EFYASIAW") AS asian_female,

        -- Black or African American
        SUM("EFYBKAAT") AS black_total,
        SUM("EFYBKAAM") AS black_male,
        SUM("EFYBKAAW") AS black_female,

        -- Hispan)ic or Latino
        SUM("EFYHISPT") AS hisp_total,
        SUM("EFYHISPM") AS hisp_male,
        SUM("EFYHISPW") AS hisp_female,

        -- Native Hawaiian or Pacific Islander
        SUM("EFYNHPIT") AS nhpi_total,
        SUM("EFYNHPIM") AS nhpi_male,
        SUM("EFYNHPIW") AS nhpi_female,

        -- White
        SUM("EFYWHITT") AS white_total,
        SUM("EFYWHITM") AS white_male,
        SUM("EFYWHITW") AS white_female,

        -- Two or More Races
        SUM("EFY2MORT") AS multi_total,
        SUM("EFY2MORM") AS multi_male,
        SUM("EFY2MORW") AS multi_female,

        -- Nonresident Alien
        SUM("EFYNRALT") AS nra_total,
        SUM("EFYNRALM") AS nra_male,
        SUM("EFYNRALW") AS nra_female,

        -- Unknown
        SUM("EFYUNKNT") AS unk_total,
        SUM("EFYUNKNM") AS unk_male,
        SUM("EFYUNKNW") AS unk_female
    FROM {{ ref('effyyyyy') }}
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