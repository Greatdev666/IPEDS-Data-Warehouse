WITH cyyyy_b AS (

    SELECT * FROM {{ ref('c2023_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2022_b') }}
    UNION ALL

    SELECT * FROM {{ ref('c2021_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2020_b') }}
    UNION ALL

    SELECT * FROM {{ ref('c2019_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2018_b') }}
    UNION ALL

    SELECT * FROM {{ ref('c2017_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2016_b') }}
    UNION ALL

    SELECT * FROM {{ ref('c2015_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2014_b') }}
    UNION ALL

    SELECT * FROM {{ ref('c2013_b') }}

    UNION ALL

    SELECT * FROM {{ ref('c2012_b') }}

)
SELECT 
    "YEAR",
    "UNITID",

    -- Grand Total
    "CSTOTLT",
    "CSTOTLM",
    "CSTOTLW",

    -- American Indian or Alaska Native
    "CSAIANT",
    "CSAIANM",
    "CSAIANW",

    -- Asian
    "CSASIAT",
    "CSASIAM",
    "CSASIAW",

    -- Black or African American
    "CSBKAAT",
    "CSBKAAM",
    "CSBKAAW",

    -- Hispanic/Latino
    "CSHISPT",
    "CSHISPM",
    "CSHISPW",

    -- Native Hawaiian or Other Pacific Islander
    "CSNHPIT",
    "CSNHPIM",
    "CSNHPIW",

    -- White
    "CSWHITT",
    "CSWHITM",
    "CSWHITW",

    -- Two or more races
    "CS2MORT",
    "CS2MORM",
    "CS2MORW",

    -- Race/ethnicity unknown
    "CSUNKNT",
    "CSUNKNM",
    "CSUNKNW",

    -- Graduate Gender Unknown
    "CSGUGUN",

    -- Gradute Another Gender
    "CSGUTOTAG",

    -- Non Resident Alien
    "CSNRALT",
    "CSNRALM",
    "CSNRALW"
FROM cyyyy_b