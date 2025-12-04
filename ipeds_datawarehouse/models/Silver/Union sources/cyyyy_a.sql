WITH final_cyyyy_a AS (

    SELECT * FROM {{ ref('c2023_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2022_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2021_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2020_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2019_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2018_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2017_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2016_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2015_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2014_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2013_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2012_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2011_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2010_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2009_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2008_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2007_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2006_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2005_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2004_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2003_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2002_a') }}
    UNION ALL

    SELECT * FROM {{ ref('c2001_a') }}

    UNION ALL

    SELECT * FROM {{ ref('c2000_a') }}
)

SELECT 
    -- =====================
    -- Identifiers
    -- =====================
    "YEAR",
    "UNITID",
    "CIPCODE",
    "MAJORNUM",
    "AWLEVEL",

    -- =====================
    -- Grand Totals
    -- =====================
    "CTOTALT",
    "CTOTALM",
    "CTOTALW",

    -- =====================
    -- American Indian / Alaska Native
    -- =====================
    "CAIANT",
    "CAIANM",
    "CAIANW",

    -- =====================
    -- Asian
    -- =====================
    "CASIAT",
    "CASIAM",
    "CASIAW",

    -- =====================
    -- Black / African American
    -- =====================
    "CBKAAT",
    "CBKAAM",
    "CBKAAW",

    -- =====================
    -- Hispanic / Latino
    -- =====================
    "CHISPT",
    "CHISPM",
    "CHISPW",

    -- =====================
    -- Native Hawaiian / Pacific Islander
    -- =====================
    "CNHPIT",
    "CNHPIM",
    "CNHPIW",

    -- =====================
    -- White
    -- =====================
    "CWHITT",
    "CWHITM",
    "CWHITW",

    -- =====================
    -- Two or More Races
    -- =====================
    "C2MORT",
    "C2MORM",
    "C2MORW",

    -- =====================
    -- Race Unknown
    -- =====================
    "CUNKNT",
    "CUNKNM",
    "CUNKNW",

    -- =====================
    -- Nonresident Alien
    -- =====================
    "CNRALT",
    "CNRALM",
    "CNRALW"

FROM final_cyyyy_a