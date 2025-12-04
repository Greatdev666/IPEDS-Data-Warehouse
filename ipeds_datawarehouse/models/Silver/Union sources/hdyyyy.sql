WITH hdyyyy AS (
    SELECT * FROM {{ ref('hd2023') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2022') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2021') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2020') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2019') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2018') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2017') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2016') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2015') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2014') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2013') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2012') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2011') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2010') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2009') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2008') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2007') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2006') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2005') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2004') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2003') }}
    
    UNION ALL
    
    SELECT * FROM {{ ref('hd2002') }}

    UNION ALL
    
    SELECT * FROM {{ ref('hd2001') }}

    UNION ALL
    
    SELECT * FROM {{ ref('hd2000') }}
)

SELECT 
    "YEAR",
    "UNITID",      -- Unique institution ID
    {{ clean_text('"INSTNM"') }} AS "INSTNM",      -- Official institution name
    {{ clean_text('"IALIAS"') }} AS "IALIAS",      -- Alias or alternative institution name

    -- Location Details
    {{ clean_text('"CITY"') }} AS "CITY",          -- City where institution is located
    "STABBR",      -- State abbreviation
    "ZIP",         -- ZIP code
    "OBEREG",      -- Census / BEA region code
    "LONGITUD",    -- Longitude coordinate
    "LATITUDE",    -- Latitude coordinate

    -- Institution Type & Control
    "SECTOR",      -- Sector (public/private/for-profit)
    "ICLEVEL",     -- Institution level (2-year, 4-year, less-than-2)
    "CONTROL",     -- Control (public/private)

    -- Programs & Degrees
    "HLOFFER",     -- Highest level of offering
    "UGOFFER",     -- Offers undergraduate programs?
    "GROFFER",     -- Offers graduate programs?
    "HDEGOFR1",    -- Highest degree offered

    -- Institution Size
    "INSTSIZE",    -- Size category of institution

    -- URLs
   {{ clean_links('"WEBADDR"') }} AS "WEBADDR",    -- Main website URL
   {{ clean_links('"ADMINURL"') }} AS "ADMINURL",    -- Admissions website URL
   {{ clean_links('"FAIDURL"') }} AS "FAIDURL"     -- Financial aid website URL

FROM hdyyyy

