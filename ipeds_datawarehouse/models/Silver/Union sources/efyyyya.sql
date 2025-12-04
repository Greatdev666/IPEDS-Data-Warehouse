WITH efyyyya AS (
    SELECT * FROM {{ ref('ef2023a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2022a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2021a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2020a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2019a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2018a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2017a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2016a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2015a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2014a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2013a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2012a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2011a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2010a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2009a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2008a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2007a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2006a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2005a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2004a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2003a') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2002a') }}

    UNION ALL

    SELECT * FROM {{ ref('ef2001a') }}

    UNION ALL

    SELECT * FROM {{ ref('ef2000a') }}
)
SELECT 
    "YEAR",
    "UNITID",
    "EFALEVEL",
    "LSTUDY",

    -- =====================
    -- Attendance Level Of Student
    -- =====================
    "SECTION",

    -- =====================
    -- Grand Totals
    -- =====================
    "EFTOTLT",
    "EFTOTLM",
    "EFTOTLW",

    -- =====================
    -- American Indian / Alaska Native
    -- =====================
    "EFAIANT",
    "EFAIANM",
    "EFAIANW",

    -- =====================
    -- Asian
    -- =====================
    "EFASIAT",
    "EFASIAM",
    "EFASIAW",

    -- =====================
    -- Black / African American
    -- =====================
    "EFBKAAT",
    "EFBKAAM",
    "EFBKAAW",

    -- =====================
    -- Hispanic / Latino
    -- =====================
    "EFHISPT",
    "EFHISPM",
    "EFHISPW",

    -- =====================
    -- Native Hawaiian / Pacific Islander
    -- =====================
    "EFNHPIT",
    "EFNHPIM",
    "EFNHPIW",

    -- =====================
    -- White
    -- =====================
    "EFWHITT",
    "EFWHITM",
    "EFWHITW",

    -- =====================
    -- Two or more races
    -- =====================
    "EF2MORT",
    "EF2MORM",
    "EF2MORW",

    -- =====================
    -- Race unknown
    -- =====================
    "EFUNKNT",
    "EFUNKNM",
    "EFUNKNW",

    -- =====================
    -- Nonresident Alien
    -- =====================
    "EFNRALT",
    "EFNRALM",
    "EFNRALW",

    -- =====================
    -- Gender Other / Unknown
    -- =====================
    "EFGNDRUN"
FROM efyyyya

