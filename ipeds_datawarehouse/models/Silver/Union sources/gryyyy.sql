WITH gryyyy AS (
    SELECT * FROM {{ ref('gr2023') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2022') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2021') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2020') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2019') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2018') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2017') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2016') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2015') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2014') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2013') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2012') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2011') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2010') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2009') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2008') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2007') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2006') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2005') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2004') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2003') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('gr2002') }}

    UNION ALL
    
    SELECT * FROM {{ ref('gr2001') }}

    UNION ALL
    
    SELECT * FROM {{ ref('gr2000') }}
)
SELECT 
    -- =====================
    -- Identifiers
    -- =====================
    "YEAR", 
    "UNITID",
    "GRTYPE",
    "CHRISTAT",
    "SECTION",
    "COHORT",
    "LINE",

    -- =====================
    -- Grand Totals
    -- =====================
    "GRTOTLT",
    "GRTOTLM",
    "GRTOTLW",

    -- =====================
    -- American Indian / Alaska Native
    -- =====================
    "GRAIANT",
    "GRAIANM",
    "GRAIANW",

    -- =====================
    -- Asian
    -- =====================
    "GRASIAT",
    "GRASIAM",
    "GRASIAW",

    -- =====================
    -- Black / African American
    -- =====================
    "GRBKAAT",
    "GRBKAAM",
    "GRBKAAW",

    -- =====================
    -- Hispanic / Latino
    -- =====================
    "GRHISPT",
    "GRHISPM",
    "GRHISPW",

    -- =====================
    -- Native Hawaiian / Pacific Islander
    -- =====================
    "GRNHPIT",
    "GRNHPIM",
    "GRNHPIW",

    -- =====================
    -- White
    -- =====================
    "GRWHITT",
    "GRWHITM",
    "GRWHITW",

    -- =====================
    -- Two or more races
    -- =====================
    "GR2MORT",
    "GR2MORM",
    "GR2MORW",

    -- =====================
    -- Race unknown
    -- =====================
    "GRUNKNT",
    "GRUNKNM",
    "GRUNKNW",

    -- =====================
    -- Nonresident Alien
    -- =====================
    "GRNRALT",
    "GRNRALM",
    "GRNRALW"

FROM gryyyy 

