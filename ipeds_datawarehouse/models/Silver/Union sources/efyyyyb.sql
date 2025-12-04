WITH efyyyyb AS (
    SELECT * FROM {{ ref('ef2023b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2022b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2021b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2020b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2019b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2018b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2017b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2016b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2015b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2014b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2013b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2012b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2011b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2010b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2009b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2008b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2007b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2006b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2005b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2004b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2003b') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('ef2002b') }}

    UNION ALL

    SELECT * FROM {{ ref('ef2001b') }}

    UNION ALL

    SELECT * FROM {{ ref('ef2000b') }}
)

SELECT 
    "YEAR",
    "UNITID",
    "LSTUDY",
    "LINE",

    -- =====================
    -- Age Category of Student
    -- =====================
    "EFBAGE",

    -- =====================
    -- Grand Totals
    -- =====================
    "EFAGE09",
    "EFAGE07",
    "EFAGE08",

    -- =====================
    -- Full Time Students
    -- =====================
    "EFAGE05",
    "EFAGE01",
    "EFAGE02",

    -- =====================
    -- Full Time Students
    -- =====================
    "EFAGE06",
    "EFAGE03",
    "EFAGE04"
        
FROM efyyyyb