WITH cyyyy_c AS (

    SELECT * FROM {{ ref('c2023_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2022_c') }}
    UNION ALL

    SELECT * FROM {{ ref('c2021_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2020_c') }}
    UNION ALL

    SELECT * FROM {{ ref('c2019_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2018_c') }}
    UNION ALL

    SELECT * FROM {{ ref('c2017_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2016_c') }}
    UNION ALL

    SELECT * FROM {{ ref('c2015_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2014_c') }}
    UNION ALL

    SELECT * FROM {{ ref('c2013_c') }}

    UNION ALL

    SELECT * FROM {{ ref('c2012_c') }}

)

SELECT 
    "YEAR",
    "UNITID",
    "AWLEVELC",

    -- Grand Total
    "CSTOTLT",
    "CSTOTLM", 
    "CSTOTLW",

    -- American Indian or Alaska Native
    "CSAIANT",

    -- Asian 
    "CSASIAT",

    -- Black or African American
    "CSBKAAT",

    -- Hispanic/Latino
    "CSHISPT",

    -- Native Hawaiian or Other Pacific Islander
    "CSNHPIT",

    -- White
    "CSWHITT",

    -- Two or more races
    "CS2MORT",

    -- Race/Ethnicity Unknown
    "CSUNKNT",

    -- Non Resident Alien
    "CSNRALT",

    -- Ages, under 18
    "CSUND18",

    -- Ages, 18-24,
    "CS18_24",

    -- Ages, 25-39
    "CS25_39",

    -- Ages, 40 and above
    "CSABV40",

    -- Ages, unknown
    "CSUNKN"

FROM cyyyy_c