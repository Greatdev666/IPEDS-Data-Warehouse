WITH effyyyyy AS (
    SELECT * FROM {{ ref('effy2023') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2022') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2021') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2020') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2019') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2018') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2017') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2016') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2015') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2014') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2013') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2012') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2011') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2010') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2009') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2008') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2007') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2006') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2005') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2004') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2003') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('effy2002') }}
)
SELECT 
    -- =====================
    -- Identifiers
    -- =====================
    "YEAR",
    "UNITID",
    "EFFYALEV",
    "EFFYLEV",
    "LSTUDY",

    -- =====================
    -- Grand Totals
    -- =====================
    "EFYTOTLT",
    "EFYTOTLM",
    "EFYTOTLW",

    -- =====================
    -- American Indian / Alaska Native
    -- =====================
    "EFYAIANT",
    "EFYAIANM",
    "EFYAIANW",

    -- =====================
    -- Asian
    -- =====================
    "EFYASIAT",
    "EFYASIAM",
    "EFYASIAW",

    -- =====================
    -- Black / African American
    -- =====================
    "EFYBKAAT",
    "EFYBKAAM",
    "EFYBKAAW",

    -- =====================
    -- Hispanic / Latino
    -- =====================
    "EFYHISPT",
    "EFYHISPM",
    "EFYHISPW",

    -- =====================
    -- Native Hawaiian / Pacific Islander
    -- =====================
    "EFYNHPIT",
    "EFYNHPIM",
    "EFYNHPIW",

    -- =====================
    -- White
    -- =====================
    "EFYWHITT",
    "EFYWHITM",
    "EFYWHITW",

    -- =====================
    -- Two or more races
    -- =====================
    "EFY2MORT",
    "EFY2MORM",
    "EFY2MORW",

    -- =====================
    -- Race unknown
    -- =====================
    "EFYUNKNT",
    "EFYUNKNM",
    "EFYUNKNW",

    -- =====================
    -- Nonresident Alien
    -- =====================
    "EFYNRALT",
    "EFYNRALM",
    "EFYNRALW",

    -- =====================
    -- Gender Other / Unknown
    -- =====================
    "EFYGUUN",
    "EFYGUAN",
    "EFYGUTOT"

FROM effyyyyy 