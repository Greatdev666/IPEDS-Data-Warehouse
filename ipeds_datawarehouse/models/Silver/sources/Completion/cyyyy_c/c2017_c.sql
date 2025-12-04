WITH c2017_c AS ( 
    SELECT * FROM {{ source('bronze', 'c2017_c') }}
)
, c2017_c_renamed AS (
    SELECT
        CAST(2017 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",
        CAST("AWLEVELC" AS INTEGER) AS "AWLEVELC",

        -- Grand Total
        CAST("CSTOTLT" AS INTEGER) AS "CSTOTLT",
        CAST("CSTOTLM" AS INTEGER) AS "CSTOTLM", 
        CAST("CSTOTLW" AS INTEGER) AS "CSTOTLW",

        -- American Indian or Alaska Native
        CAST("CSAIANT" AS INTEGER) AS "CSAIANT",

        -- Asian 
        CAST("CSASIAT" AS INTEGER) AS "CSASIAT",

        -- Black or African American
        CAST("CSBKAAT" AS INTEGER) AS "CSBKAAT",

        -- Hispanic/Latino
        CAST("CSHISPT" AS INTEGER) AS "CSHISPT",

        -- Native Hawaiian or Other Pacific Islander
        CAST("CSNHPIT" AS INTEGER) AS "CSNHPIT",

        -- White
        CAST("CSWHITT" AS INTEGER) AS "CSWHITT",

        -- Two or more races
        CAST("CS2MORT" AS INTEGER) AS "CS2MORT",

        -- Race/Ethnicity Unknown
        CAST("CSUNKNT" AS INTEGER) AS "CSUNKNT",

        -- Non Resident Alien
        CAST("CSNRALT" AS INTEGER) AS "CSNRALT",

        -- Ages, under 18
        CAST("CSUND18" AS INTEGER) AS "CSUND18",

        -- Ages, 18-24,
        CAST("CS18_24" AS INTEGER) AS "CS18_24",

        -- Ages, 25-39
        CAST("CS25_39" AS INTEGER) AS "CS25_39",

        -- Ages, 40 and above
        CAST("CSABV40" AS INTEGER)"CSABV40",

        -- Ages, unknown
        CAST("CSUNKN" AS INTEGER) AS "CSUNKN"

    FROM c2017_c
)
SELECT * FROM c2017_c_renamed